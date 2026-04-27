#!/usr/bin/env bash
# =============================================================================
# get_datastores.sh
# Lists datastore capacity, used, and free space (TiB) for a given vCenter
# cluster. Handles cluster/datacenter names with spaces safely.
#
# Requirements : govc (brew install govc), jq (brew install jq)
# Operations   : READ-ONLY — no writes, no state changes in vCenter
# Usage        : ./get_datastores.sh -c <cluster_name> [-s <sort_by>]
# =============================================================================

set -euo pipefail

# --------------------------------------------------------------------------
# vCenter credentials — set via environment or export before running
# --------------------------------------------------------------------------
GOVC_URL="${GOVC_URL:-}"
GOVC_USERNAME="${GOVC_USERNAME:-}"
GOVC_PASSWORD="${GOVC_PASSWORD:-}"
GOVC_INSECURE="${GOVC_INSECURE:-true}"   # set false if vCenter has valid CA cert

export GOVC_URL GOVC_USERNAME GOVC_PASSWORD GOVC_INSECURE

# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------
CLUSTER=""
SORT_BY="free"   # options: name | capacity | used | free

# --------------------------------------------------------------------------
# Usage
# --------------------------------------------------------------------------
usage() {
  echo ""
  echo "Usage: $(basename "$0") -c <cluster_name> [-s <sort_by>]"
  echo ""
  echo "  -c  Cluster name as it appears in vCenter (required)"
  echo "      Handles names with spaces — wrap in quotes: -c 'My Cluster 01'"
  echo "  -s  Sort by: name | capacity | used | free  (default: free)"
  echo ""
  echo "Environment variables (must be set before running):"
  echo "  GOVC_URL        vCenter URL        e.g. https://vcenter.example.com"
  echo "  GOVC_USERNAME   vCenter username   e.g. administrator@vsphere.local"
  echo "  GOVC_PASSWORD   vCenter password"
  echo ""
  echo "Examples:"
  echo "  ./get_datastores.sh -c 'DC1-APP-W1'"
  echo "  ./get_datastores.sh -c 'My Cluster 01' -s capacity"
  echo ""
  exit 1
}

# --------------------------------------------------------------------------
# Parse arguments
# --------------------------------------------------------------------------
while getopts ":c:s:h" opt; do
  case $opt in
    c) CLUSTER="$OPTARG" ;;
    s) SORT_BY="$OPTARG" ;;
    h) usage ;;
    *) echo "Unknown option: -$OPTARG"; usage ;;
  esac
done

# --------------------------------------------------------------------------
# Validate inputs
# --------------------------------------------------------------------------
[[ -z "$CLUSTER" ]]       && echo "[ERROR] Cluster name is required. Use -c <cluster_name>" && usage
[[ -z "$GOVC_URL" ]]      && echo "[ERROR] GOVC_URL is not set."      && exit 1
[[ -z "$GOVC_USERNAME" ]] && echo "[ERROR] GOVC_USERNAME is not set." && exit 1
[[ -z "$GOVC_PASSWORD" ]] && echo "[ERROR] GOVC_PASSWORD is not set." && exit 1

if ! command -v govc &>/dev/null; then
  echo "[ERROR] govc is not installed or not in PATH."
  echo "        Install via: brew install govc"
  exit 1
fi

if ! command -v jq &>/dev/null; then
  echo "[ERROR] jq is not installed or not in PATH."
  echo "        Install via: brew install jq"
  exit 1
fi

if ! [[ "$SORT_BY" =~ ^(name|capacity|used|free)$ ]]; then
  echo "[ERROR] Invalid sort option '$SORT_BY'. Choose from: name | capacity | used | free"
  exit 1
fi

# --------------------------------------------------------------------------
# Helper: convert bytes to TiB
# --------------------------------------------------------------------------
to_tib() {
  awk "BEGIN { printf \"%.2f\", $1 / (1024^4) }"
}

# --------------------------------------------------------------------------
# Step 1: Resolve cluster — fetch ALL clusters as JSON, match by name
#
# Why JSON + jq instead of plain text grep?
# govc find returns paths like: ./host/DC One/My Cluster 01
# Shell word-splitting on spaces breaks grep and variable passing.
# JSON output keeps each object as a structured record — spaces in names
# are handled natively by jq without any quoting workarounds.
# --------------------------------------------------------------------------
echo ""
echo "Connecting to : $GOVC_URL"
echo "Cluster       : $CLUSTER"
echo "Sorted by     : $SORT_BY"
echo ""
echo "Resolving cluster path..."

# -json returns a JSON array; each element has a "Path" field
ALL_CLUSTERS_JSON=$(govc find . -type ClusterComputeResource -json 2>/dev/null)

if [[ -z "$ALL_CLUSTERS_JSON" || "$ALL_CLUSTERS_JSON" == "null" ]]; then
  echo "[ERROR] Could not retrieve cluster list from vCenter. Check credentials and URL."
  exit 1
fi

# Match cluster whose path ends with /<CLUSTER> or equals <CLUSTER> exactly
# Using jq's test() with a regex so spaces in the name are matched literally
CLUSTER_PATH=$(echo "$ALL_CLUSTERS_JSON" | \
  jq -r --arg name "$CLUSTER" \
  '.[] | select(.Path | test("(^|/)" + ($name | gsub("\\s"; "\\s")) + "$")) | .Path' \
  2>/dev/null | head -1)

if [[ -z "$CLUSTER_PATH" ]]; then
  echo "[ERROR] Cluster '$CLUSTER' not found in vCenter."
  echo ""
  echo "Available clusters:"
  echo "$ALL_CLUSTERS_JSON" | jq -r '.[].Path' 2>/dev/null | sed 's/^/  /'
  exit 1
fi

echo "Resolved path : $CLUSTER_PATH"
echo ""

# --------------------------------------------------------------------------
# Step 2: Get ESXi hosts in the cluster (JSON, safe for spaces)
# --------------------------------------------------------------------------
echo "Fetching hosts in cluster..."

HOST_PATHS_JSON=$(govc find "$CLUSTER_PATH" -type h -json 2>/dev/null)

if [[ -z "$HOST_PATHS_JSON" || "$HOST_PATHS_JSON" == "null" || "$HOST_PATHS_JSON" == "[]" ]]; then
  echo "[ERROR] No ESXi hosts found under cluster '$CLUSTER_PATH'."
  exit 1
fi

# Extract host paths into a bash array (newline-delimited, handles spaces)
mapfile -t HOST_PATHS < <(echo "$HOST_PATHS_JSON" | jq -r '.[].Path')

echo "Found ${#HOST_PATHS[@]} host(s). Collecting datastores..."
echo ""

# --------------------------------------------------------------------------
# Step 3: Collect unique datastore paths across all hosts (JSON, safe)
# Each host returns its mounted datastores; we deduplicate across hosts.
# --------------------------------------------------------------------------
declare -A SEEN_DS
DS_NAME_LIST=()

for host in "${HOST_PATHS[@]}"; do
  DS_JSON=$(govc find "$host" -type s -json 2>/dev/null) || continue
  [[ -z "$DS_JSON" || "$DS_JSON" == "null" || "$DS_JSON" == "[]" ]] && continue

  while IFS= read -r ds_path; do
    # Extract the datastore name = last segment after /datastore/
    ds_name="${ds_path##*/datastore/}"
    if [[ -z "${SEEN_DS[$ds_name]+_}" ]]; then
      SEEN_DS["$ds_name"]=1
      DS_NAME_LIST+=("$ds_name")
    fi
  done < <(echo "$DS_JSON" | jq -r '.[].Path')
done

if [[ ${#DS_NAME_LIST[@]} -eq 0 ]]; then
  echo "[ERROR] No datastores found for cluster '$CLUSTER'."
  exit 1
fi

echo "Found ${#DS_NAME_LIST[@]} unique datastore(s). Fetching capacity details..."
echo ""

# --------------------------------------------------------------------------
# Step 4: Fetch datastore info as JSON — one call per datastore
# govc datastore.info -json handles names with spaces correctly when quoted
# --------------------------------------------------------------------------
OUTPUT_LINES=()

for ds_name in "${DS_NAME_LIST[@]}"; do
  DS_INFO_JSON=$(govc datastore.info -json "$ds_name" 2>/dev/null) || continue
  [[ -z "$DS_INFO_JSON" || "$DS_INFO_JSON" == "null" ]] && continue

  # Parse capacity and free bytes directly from JSON — no text parsing needed
  CAP_BYTES=$(echo "$DS_INFO_JSON"  | jq -r '.[0].Summary.Capacity // 0')
  FREE_BYTES=$(echo "$DS_INFO_JSON" | jq -r '.[0].Summary.FreeSpace // 0')

  [[ "$CAP_BYTES" -eq 0 ]] && continue   # skip if no data returned

  CAP_TIB=$(to_tib "$CAP_BYTES")
  FREE_TIB=$(to_tib "$FREE_BYTES")
  USED_TIB=$(awk "BEGIN { printf \"%.2f\", $CAP_TIB - $FREE_TIB }")
  FREE_PCT=$(awk "BEGIN { printf \"%.1f\", ($FREE_TIB / $CAP_TIB) * 100 }")

  # Pipe-delimited row: name|capacity|used|free|free_pct
  OUTPUT_LINES+=("${ds_name}|${CAP_TIB}|${USED_TIB}|${FREE_TIB}|${FREE_PCT}")
done

if [[ ${#OUTPUT_LINES[@]} -eq 0 ]]; then
  echo "[ERROR] Datastores were found but capacity details could not be retrieved."
  exit 1
fi

# --------------------------------------------------------------------------
# Sort
# --------------------------------------------------------------------------
case "$SORT_BY" in
  name)     SORT_FIELD=1; SORT_FLAG="" ;;
  capacity) SORT_FIELD=2; SORT_FLAG="-rn" ;;
  used)     SORT_FIELD=3; SORT_FLAG="-rn" ;;
  free)     SORT_FIELD=4; SORT_FLAG="-rn" ;;
esac

SORTED=$(printf '%s\n' "${OUTPUT_LINES[@]}" | sort ${SORT_FLAG:-} -t'|' -k"$SORT_FIELD")

# --------------------------------------------------------------------------
# Print table
# --------------------------------------------------------------------------
HEADER=$(printf "%-50s %13s %12s %12s %8s" "DATASTORE" "CAPACITY(TiB)" "USED(TiB)" "FREE(TiB)" "FREE%")
DIVIDER=$(printf '%0.s-' {1..100})

echo "$HEADER"
echo "$DIVIDER"

while IFS='|' read -r name cap used free pct; do
  flag=""
  if awk "BEGIN { exit !($pct < 20) }"; then flag="  [!!]"; fi

  printf "%-50s %13s %12s %12s %7s%%%s\n" \
    "$name" "$cap" "$used" "$free" "$pct" "$flag"
done <<< "$SORTED"

echo "$DIVIDER"
echo ""
echo "  [!!] = Less than 20% free space — avoid for new VM provisioning"
echo ""
echo "Total datastores : ${#OUTPUT_LINES[@]}"
echo "Cluster          : $CLUSTER_PATH"
echo ""
