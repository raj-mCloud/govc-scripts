#!/usr/bin/env bash
# =============================================================================
# get_datastores.sh
# Lists datastores for a given vCenter cluster sorted by free space (highest
# first) — ready to pick for VM provisioning.
#
# Requirements : govc (brew install govc), jq (brew install jq)
# Operations   : READ-ONLY — no writes, no state changes in vCenter
# Usage        : ./get_datastores.sh -c <cluster_name>
# =============================================================================

set -euo pipefail

# --------------------------------------------------------------------------
# vCenter credentials — export before running or set inline
# --------------------------------------------------------------------------
GOVC_URL="${GOVC_URL:-}"
GOVC_USERNAME="${GOVC_USERNAME:-}"
GOVC_PASSWORD="${GOVC_PASSWORD:-}"
GOVC_INSECURE="${GOVC_INSECURE:-true}"   # set false if vCenter has a valid CA cert

export GOVC_URL GOVC_USERNAME GOVC_PASSWORD GOVC_INSECURE

CLUSTER=""

# --------------------------------------------------------------------------
# Usage
# --------------------------------------------------------------------------
usage() {
  echo ""
  echo "Usage: $(basename "$0") -c <cluster_name>"
  echo ""
  echo "  -c  Cluster name as shown in vCenter (required)"
  echo "      Names with spaces must be quoted: -c 'My Cluster 01'"
  echo ""
  echo "Environment variables (must be exported before running):"
  echo "  GOVC_URL        e.g. https://vcenter.example.com"
  echo "  GOVC_USERNAME   e.g. administrator@vsphere.local"
  echo "  GOVC_PASSWORD"
  echo ""
  echo "Examples:"
  echo "  ./get_datastores.sh -c 'DC1-APP-W1'"
  echo "  ./get_datastores.sh -c 'My Cluster 01'"
  echo ""
  exit 1
}

# --------------------------------------------------------------------------
# Parse arguments
# --------------------------------------------------------------------------
while getopts ":c:h" opt; do
  case $opt in
    c) CLUSTER="$OPTARG" ;;
    h) usage ;;
    *) echo "Unknown option: -$OPTARG"; usage ;;
  esac
done

# --------------------------------------------------------------------------
# Validate
# --------------------------------------------------------------------------
[[ -z "$CLUSTER" ]]       && echo "[ERROR] -c <cluster_name> is required." && usage
[[ -z "$GOVC_URL" ]]      && echo "[ERROR] GOVC_URL is not set."      && exit 1
[[ -z "$GOVC_USERNAME" ]] && echo "[ERROR] GOVC_USERNAME is not set." && exit 1
[[ -z "$GOVC_PASSWORD" ]] && echo "[ERROR] GOVC_PASSWORD is not set." && exit 1

command -v govc &>/dev/null || { echo "[ERROR] govc not found. Install: brew install govc"; exit 1; }
command -v jq   &>/dev/null || { echo "[ERROR] jq not found.   Install: brew install jq";   exit 1; }

# --------------------------------------------------------------------------
# Helper: bytes -> TiB
# --------------------------------------------------------------------------
to_tib() {
  awk "BEGIN { printf \"%.2f\", $1 / (1024^4) }"
}

# --------------------------------------------------------------------------
# Step 1: Resolve the full inventory path for the given cluster name.
#
# govc find outputs one inventory path per line (plain text only — -json
# flag is not implemented for find). Using `while IFS= read -r` reads each
# line verbatim so spaces in DC/cluster names are preserved correctly.
# Match on the last path segment only for an exact cluster name match.
# --------------------------------------------------------------------------
echo ""
echo "Connecting to : $GOVC_URL"
echo "Cluster       : $CLUSTER"
echo ""
echo "Resolving cluster path..."

CLUSTER_PATH=""
while IFS= read -r line; do
  segment="${line##*/}"
  if [[ "$segment" == "$CLUSTER" ]]; then
    CLUSTER_PATH="$line"
    break
  fi
done < <(govc find / -type ClusterComputeResource 2>/dev/null)

if [[ -z "$CLUSTER_PATH" ]]; then
  echo "[ERROR] Cluster '$CLUSTER' not found."
  echo ""
  echo "Available clusters:"
  govc find / -type ClusterComputeResource 2>/dev/null | sed 's/^/  /'
  exit 1
fi

echo "Resolved path : $CLUSTER_PATH"
echo ""

# --------------------------------------------------------------------------
# Step 2: Collect unique datastore full paths from all hosts in the cluster.
#
# govc find receives each path as a properly quoted single argument, so
# spaces in any part of the path are handled correctly by the shell.
# Deduplication via associative array prevents counting shared datastores
# (e.g. NFS/vSAN) multiple times when mounted across several hosts.
# --------------------------------------------------------------------------
echo "Fetching hosts in cluster..."

declare -a HOST_LIST=()
while IFS= read -r host; do
  [[ -n "$host" ]] && HOST_LIST+=("$host")
done < <(govc find "$CLUSTER_PATH" -type h 2>/dev/null)

if [[ ${#HOST_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No ESXi hosts found under '$CLUSTER_PATH'."
  exit 1
fi

echo "Found ${#HOST_LIST[@]} host(s). Collecting datastores..."

declare -A SEEN_DS=()
declare -a DS_PATH_LIST=()

for host in "${HOST_LIST[@]}"; do
  while IFS= read -r ds_path; do
    [[ -z "$ds_path" ]] && continue
    if [[ -z "${SEEN_DS[$ds_path]+_}" ]]; then
      SEEN_DS["$ds_path"]=1
      DS_PATH_LIST+=("$ds_path")
    fi
  done < <(govc find "$host" -type s 2>/dev/null)
done

if [[ ${#DS_PATH_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No datastores found for cluster '$CLUSTER'."
  exit 1
fi

echo "Found ${#DS_PATH_LIST[@]} unique datastore(s). Fetching capacity..."
echo ""

# --------------------------------------------------------------------------
# Step 3: Fetch capacity for each datastore via its full inventory path.
#
# govc ls -json "<full_path>" accepts the path as a single quoted argument
# (space-safe) and returns Summary.Capacity / Summary.FreeSpace as raw bytes.
# This is more reliable than govc datastore.info which takes bare names and
# breaks on spaces and special characters.
# --------------------------------------------------------------------------
OUTPUT_LINES=()

for ds_path in "${DS_PATH_LIST[@]}"; do
  ds_name="${ds_path##*/}"

  DS_JSON=$(govc ls -json "$ds_path" 2>/dev/null) || continue
  [[ -z "$DS_JSON" || "$DS_JSON" == "null" ]] && continue

  CAP_BYTES=$(echo  "$DS_JSON" | jq -r '.elements[0].Object.Summary.Capacity  // 0' 2>/dev/null)
  FREE_BYTES=$(echo "$DS_JSON" | jq -r '.elements[0].Object.Summary.FreeSpace // 0' 2>/dev/null)

  [[ -z "$CAP_BYTES"  || "$CAP_BYTES"  == "null" || "$CAP_BYTES"  -eq 0 ]] && continue
  [[ -z "$FREE_BYTES" || "$FREE_BYTES" == "null" ]] && continue

  CAP_TIB=$(to_tib  "$CAP_BYTES")
  FREE_TIB=$(to_tib "$FREE_BYTES")
  USED_TIB=$(awk "BEGIN { printf \"%.2f\", $CAP_TIB - $FREE_TIB }")
  FREE_PCT=$(awk "BEGIN { printf \"%.1f\", ($FREE_TIB / $CAP_TIB) * 100 }")

  # Stored as: free_bytes|name|capacity|used|free|free_pct
  # free_bytes as first field gives numeric sort accuracy (avoids TiB float sort issues)
  OUTPUT_LINES+=("${FREE_BYTES}|${ds_name}|${CAP_TIB}|${USED_TIB}|${FREE_TIB}|${FREE_PCT}")
done

if [[ ${#OUTPUT_LINES[@]} -eq 0 ]]; then
  echo "[ERROR] Could not retrieve capacity for any datastore. Check connectivity."
  exit 1
fi

# --------------------------------------------------------------------------
# Sort by free bytes descending (highest free space first) — fixed, no flag
# --------------------------------------------------------------------------
SORTED=$(printf '%s\n' "${OUTPUT_LINES[@]}" | sort -rn -t'|' -k1)

# --------------------------------------------------------------------------
# Print table
# --------------------------------------------------------------------------
DIVIDER=$(printf '%0.s-' {1..97})

printf "\n"
printf "%-50s %13s %12s %12s %8s\n" "DATASTORE" "CAPACITY(TiB)" "USED(TiB)" "FREE(TiB)" "FREE%"
echo "$DIVIDER"

while IFS='|' read -r _bytes name cap used free pct; do
  flag=""
  awk "BEGIN { exit !($pct < 20) }" && flag="  [!!]"
  printf "%-50s %13s %12s %12s %7s%%%s\n" "$name" "$cap" "$used" "$free" "$pct" "$flag"
done <<< "$SORTED"

echo "$DIVIDER"
printf "\n"
printf "  [!!] = Less than 20%% free — avoid for new VM provisioning\n"
printf "  Output sorted by free space — highest available on top\n"
printf "\n"
printf "  Total datastores : %s\n" "${#OUTPUT_LINES[@]}"
printf "  Cluster          : %s\n" "$CLUSTER_PATH"
printf "\n"
