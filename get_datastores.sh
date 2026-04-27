#!/usr/bin/env bash
# =============================================================================
# get_datastores.sh
# Lists datastore capacity, free, and used space for a given vCenter cluster.
# Requirements: govc CLI installed and available in PATH
# Usage: ./get_datastores.sh -c <cluster_name> [-s <sort_by>]
# =============================================================================

set -euo pipefail

# --------------------------------------------------------------------------
# Configuration — set these via environment or edit defaults below
# --------------------------------------------------------------------------
GOVC_URL="${GOVC_URL:-}"
GOVC_USERNAME="${GOVC_USERNAME:-}"
GOVC_PASSWORD="${GOVC_PASSWORD:-}"
GOVC_INSECURE="${GOVC_INSECURE:-true}"   # set to false if you have valid certs

export GOVC_URL GOVC_USERNAME GOVC_PASSWORD GOVC_INSECURE

# --------------------------------------------------------------------------
# Defaults
# --------------------------------------------------------------------------
CLUSTER=""
SORT_BY="free"   # options: name, capacity, used, free

# --------------------------------------------------------------------------
# Usage
# --------------------------------------------------------------------------
usage() {
  echo ""
  echo "Usage: $(basename "$0") -c <cluster_name> [-s <sort_by>]"
  echo ""
  echo "  -c  Cluster name as it appears in vCenter (required)"
  echo "  -s  Sort output by: name | capacity | used | free  (default: free)"
  echo ""
  echo "Environment variables (required if not already exported):"
  echo "  GOVC_URL        vCenter URL, e.g. https://vcenter.example.com"
  echo "  GOVC_USERNAME   vCenter username, e.g. administrator@vsphere.local"
  echo "  GOVC_PASSWORD   vCenter password"
  echo ""
  echo "Example:"
  echo "  GOVC_URL=https://vc.lab.local GOVC_USERNAME=admin@vsphere.local \\"
  echo "  GOVC_PASSWORD=secret ./get_datastores.sh -c 'Cluster-Prod-01' -s free"
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

if ! [[ "$SORT_BY" =~ ^(name|capacity|used|free)$ ]]; then
  echo "[ERROR] Invalid sort option '$SORT_BY'. Choose from: name | capacity | used | free"
  exit 1
fi

# --------------------------------------------------------------------------
# Helper: convert bytes to TiB (rounded to 2 decimal places)
# --------------------------------------------------------------------------
to_tib() {
  local bytes="$1"
  # govc returns bytes; convert to TiB
  awk "BEGIN { printf \"%.2f\", $bytes / (1024^4) }"
}

# --------------------------------------------------------------------------
# Fetch datastores for the given cluster
# --------------------------------------------------------------------------
echo ""
echo "Connecting to: $GOVC_URL"
echo "Cluster      : $CLUSTER"
echo "Sorted by    : $SORT_BY"
echo ""

# Get all datastores associated with the cluster
# govc finds datastores via the cluster's host members
DATASTORES=$(govc datastore.info -dc "" $(govc find . -type s -parent "/*/host/$CLUSTER" 2>/dev/null | sed 's|.*/datastore/||') 2>/dev/null) || true

# Fallback: directly query datastores under the cluster path
if [[ -z "$DATASTORES" ]]; then
  DS_PATHS=$(govc find . -type s -parent "*/$CLUSTER" 2>/dev/null) || true
  if [[ -z "$DS_PATHS" ]]; then
    echo "[ERROR] No datastores found for cluster '$CLUSTER'."
    echo "        Verify the cluster name with: govc find . -type ClusterComputeResource"
    exit 1
  fi
  DATASTORES=$(govc datastore.info $DS_PATHS 2>/dev/null)
fi

# --------------------------------------------------------------------------
# Parse and format output
# --------------------------------------------------------------------------
# govc datastore.info returns blocks like:
#   Name: ds_name
#   ...
#   Capacity: X
#   Free: Y
#   ...

OUTPUT_LINES=()

while IFS= read -r line; do
  if [[ "$line" =~ ^Name:[[:space:]]+(.+)$ ]];     then DS_NAME="${BASH_REMATCH[1]}"; fi
  if [[ "$line" =~ ^[[:space:]]*Capacity:[[:space:]]+([0-9]+)$ ]]; then DS_CAP="${BASH_REMATCH[1]}"; fi
  if [[ "$line" =~ ^[[:space:]]*Free:[[:space:]]+([0-9]+)$ ]];     then DS_FREE="${BASH_REMATCH[1]}"; fi

  # When we have all three values for a datastore, emit a row
  if [[ -n "${DS_NAME:-}" && -n "${DS_CAP:-}" && -n "${DS_FREE:-}" ]]; then
    CAP_TIB=$(to_tib "$DS_CAP")
    FREE_TIB=$(to_tib "$DS_FREE")
    USED_TIB=$(awk "BEGIN { printf \"%.2f\", $CAP_TIB - $FREE_TIB }")
    FREE_PCT=$(awk "BEGIN { printf \"%.1f\", ($FREE_TIB / $CAP_TIB) * 100 }")

    # Store as pipe-delimited for sorting: name|cap|used|free|free_pct
    OUTPUT_LINES+=("${DS_NAME}|${CAP_TIB}|${USED_TIB}|${FREE_TIB}|${FREE_PCT}")

    # Reset for next datastore
    DS_NAME=""; DS_CAP=""; DS_FREE=""
  fi
done <<< "$DATASTORES"

if [[ ${#OUTPUT_LINES[@]} -eq 0 ]]; then
  echo "[WARN] Datastores were found but could not be parsed. Raw output:"
  echo "$DATASTORES"
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

SORTED=$(printf '%s\n' "${OUTPUT_LINES[@]}" | sort $SORT_FLAG -t'|' -k"$SORT_FIELD")

# --------------------------------------------------------------------------
# Print table
# --------------------------------------------------------------------------
HEADER=$(printf "%-45s %12s %12s %12s %8s" "DATASTORE" "CAPACITY(TiB)" "USED(TiB)" "FREE(TiB)" "FREE%")
DIVIDER=$(printf '%0.s-' {1..95})

echo "$HEADER"
echo "$DIVIDER"

while IFS='|' read -r name cap used free pct; do
  # Flag datastores with less than 20% free
  flag=""
  if awk "BEGIN { exit !($pct < 20) }"; then flag=" !!"; fi

  printf "%-45s %12s %12s %12s %7s%%%s\n" \
    "$name" "$cap" "$used" "$free" "$pct" "$flag"
done <<< "$SORTED"

echo "$DIVIDER"
echo ""
echo "  !! = Less than 20% free space remaining"
echo ""
echo "Total datastores listed: ${#OUTPUT_LINES[@]}"
echo ""
