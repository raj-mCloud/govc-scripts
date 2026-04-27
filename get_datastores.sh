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
GOVC_INSECURE="${GOVC_INSECURE:-true}"

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
# Step 1: Resolve cluster path and extract the datacenter name from it.
#
# govc find returns paths like: /DC1/host/DC1-APP-W1
# The datacenter is always the first segment after the leading slash.
# We match the last path segment against the given cluster name.
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

# Extract datacenter: /DC1/host/DC1-APP-W1 -> DC1
# Strip leading slash, take the first segment
DC_NAME="${CLUSTER_PATH#/}"       # remove leading /
DC_NAME="${DC_NAME%%/*}"          # take everything before the first /

echo "Datacenter    : $DC_NAME"
echo ""

# --------------------------------------------------------------------------
# Step 2: Find all datastores under the datacenter's datastore folder.
#
# In vCenter's inventory, datastores are NOT children of host objects.
# They live under /<DC>/datastore/ regardless of which cluster uses them.
# We fetch all datastores in the DC, then filter to only those accessible
# by hosts in our cluster — determined via govc datastore.info -json which
# includes the list of hosts that have access to each datastore.
#
# This is the correct and reliable inventory traversal pattern.
# --------------------------------------------------------------------------
echo "Fetching hosts in cluster..."

DC_DS_FOLDER="/${DC_NAME}/datastore"

HOST_LIST=()
while IFS= read -r host; do
  [[ -n "$host" ]] && HOST_LIST+=("$host")
done < <(govc find "$CLUSTER_PATH" -type h 2>/dev/null)

if [[ ${#HOST_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No ESXi hosts found under '$CLUSTER_PATH'."
  exit 1
fi

echo "Found ${#HOST_LIST[@]} host(s)."
echo "Fetching datastores from datacenter folder: $DC_DS_FOLDER"
echo ""

# Build a newline-delimited list of short host names for matching
# govc reports host paths like /DC1/host/Cluster/esxi01.domain.com
# The hostname is the last segment
HOST_NAMES=""
for h in "${HOST_LIST[@]}"; do
  HOST_NAMES="${HOST_NAMES}${h##*/}"$'\n'
done

# --------------------------------------------------------------------------
# Step 3: List all datastores in the DC datastore folder, then for each
# one use govc datastore.info -json to check if any of our cluster hosts
# have access to it. This correctly scopes datastores to the cluster.
# --------------------------------------------------------------------------
DS_PATH_LIST=()
DS_SEEN_LOG=""

while IFS= read -r ds_path; do
  [[ -z "$ds_path" ]] && continue
  if ! printf '%s' "$DS_SEEN_LOG" | grep -qxF "$ds_path"; then
    DS_SEEN_LOG="${DS_SEEN_LOG}${ds_path}"$'\n'
    DS_PATH_LIST+=("$ds_path")
  fi
done < <(govc find "$DC_DS_FOLDER" -type s 2>/dev/null)

if [[ ${#DS_PATH_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No datastores found under '$DC_DS_FOLDER'."
  echo "        Verify with: govc find /${DC_NAME} -type s"
  exit 1
fi

echo "Found ${#DS_PATH_LIST[@]} datastore(s) in datacenter. Filtering to cluster '$CLUSTER'..."
echo ""

# --------------------------------------------------------------------------
# Step 4: For each datastore, fetch capacity via govc ls -json <full_path>.
# Filter: only include datastores accessible by at least one host in our
# cluster by checking the host list in the datastore's summary JSON.
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

  OUTPUT_LINES+=("${FREE_BYTES}|${ds_name}|${CAP_TIB}|${USED_TIB}|${FREE_TIB}|${FREE_PCT}")
done

if [[ ${#OUTPUT_LINES[@]} -eq 0 ]]; then
  echo "[ERROR] Could not retrieve capacity for any datastore. Check connectivity."
  exit 1
fi

# --------------------------------------------------------------------------
# Sort by free bytes descending — highest free space on top
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
printf "  Sorted by free space — highest available on top\n"
printf "\n"
printf "  Total datastores : %s\n" "${#OUTPUT_LINES[@]}"
printf "  Cluster          : %s\n" "$CLUSTER_PATH"
printf "\n"
