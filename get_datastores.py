#!/usr/bin/env python3
"""
get_datastores.py
Lists datastores for a given vCenter cluster sorted by free space (highest first).

Core logic:
  1. Find cluster by name match
  2. Extract datacenter from cluster's parent chain
  3. Get ESXi hosts under the cluster
  4. Get datastores under the datacenter's datastore folder
  5. Bulk-fetch Capacity + FreeSpace in a single Property Collector API call
  6. Print table sorted by free space descending

Requirements : pip install pyVmomi
Operations   : READ-ONLY
Usage        : python3 get_datastores.py -c <cluster_name>
"""

import argparse
import os
import ssl
import sys

try:
    from pyVim.connect import SmartConnect, Disconnect
    from pyVmomi import vim, vmodl
except ImportError:
    print("[ERROR] pyVmomi not installed. Run: pip install pyVmomi")
    sys.exit(1)


# --------------------------------------------------------------------------
# Args
# --------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="List datastores for a vCenter cluster sorted by free space."
    )
    parser.add_argument("-c", "--cluster", required=True,
                        help="Cluster name as shown in vCenter. Quote names with spaces.")
    parser.add_argument("--url",      default=os.environ.get("GOVC_URL", ""))
    parser.add_argument("--username", default=os.environ.get("GOVC_USERNAME", ""))
    parser.add_argument("--password", default=os.environ.get("GOVC_PASSWORD", ""))
    parser.add_argument("--no-verify-ssl", action="store_true",
                        default=(os.environ.get("GOVC_INSECURE", "true").lower() == "true"))
    return parser.parse_args()


# --------------------------------------------------------------------------
# Connect
# --------------------------------------------------------------------------
def connect(url, username, password, skip_ssl):
    host = url.replace("https://", "").replace("http://", "").rstrip("/")
    context = None
    if skip_ssl:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        return SmartConnect(host=host, user=username, pwd=password, sslContext=context)
    except vim.fault.InvalidLogin:
        print("[ERROR] Invalid vCenter credentials.")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Could not connect to vCenter: {e}")
        sys.exit(1)


# --------------------------------------------------------------------------
# Step 1 + 2: Find cluster by name, and get its parent Datacenter object.
#
# CreateContainerView fetches all ClusterComputeResource objects in one
# call. We then walk each cluster's parent chain to reach the Datacenter.
# Match is done on cluster.name exactly — handles spaces natively since
# these are Python string comparisons, not shell word-splitting.
# --------------------------------------------------------------------------
def find_cluster_and_dc(content, cluster_name):
    container = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.ClusterComputeResource], recursive=True
    )
    all_clusters = list(container.view)
    container.Destroy()

    matched = None
    for cluster in all_clusters:
        if cluster.name == cluster_name:
            matched = cluster
            break

    if matched is None:
        print(f"[ERROR] Cluster '{cluster_name}' not found.\n")
        print("Available clusters:")
        for c in all_clusters:
            print(f"  {c.name}")
        sys.exit(1)

    # Walk parent chain to find the Datacenter
    # vCenter object hierarchy: Datacenter > host folder > ClusterComputeResource
    dc = None
    parent = matched.parent
    while parent:
        if isinstance(parent, vim.Datacenter):
            dc = parent
            break
        parent = parent.parent

    if dc is None:
        print(f"[ERROR] Could not resolve datacenter for cluster '{cluster_name}'.")
        sys.exit(1)

    return matched, dc


# --------------------------------------------------------------------------
# Step 3: Get all ESXi hosts under the cluster.
# cluster.host is a direct property — no extra API call needed.
# --------------------------------------------------------------------------
def get_hosts(cluster):
    return list(cluster.host)


# --------------------------------------------------------------------------
# Step 4: Get all datastores under the datacenter's datastore folder.
# Walks dc.datastoreFolder recursively to handle nested folders and
# StoragePod (datastore cluster) children — same scope as the shell
# script's govc find "/<DC>/datastore" -type s.
# --------------------------------------------------------------------------
def get_datastores_in_dc(dc):
    datastores = []
    seen = set()

    def walk(folder):
        for child in folder.childEntity:
            if isinstance(child, vim.Datastore):
                if child._moId not in seen:
                    seen.add(child._moId)
                    datastores.append(child)
            elif isinstance(child, vim.StoragePod):
                for ds in child.childEntity:
                    if isinstance(ds, vim.Datastore) and ds._moId not in seen:
                        seen.add(ds._moId)
                        datastores.append(ds)
            elif isinstance(child, vim.Folder):
                walk(child)

    walk(dc.datastoreFolder)
    return datastores


# --------------------------------------------------------------------------
# Step 5: Bulk-fetch Capacity + FreeSpace for all datastores in ONE call.
#
# The Property Collector accepts a list of object references and property
# paths, and returns all values in a single vSphere API round trip.
# This replaces the shell script's per-datastore serial govc ls -json calls
# and is the primary performance gain regardless of datastore count.
# --------------------------------------------------------------------------
def fetch_capacities(content, ds_objects):
    prop_spec = vmodl.query.PropertyCollector.PropertySpec(
        type=vim.Datastore,
        pathSet=["summary.name", "summary.capacity",
                 "summary.freeSpace", "summary.accessible"]
    )
    obj_specs = [
        vmodl.query.PropertyCollector.ObjectSpec(obj=ds)
        for ds in ds_objects
    ]
    filter_spec = vmodl.query.PropertyCollector.FilterSpec(
        objectSet=obj_specs,
        propSet=[prop_spec]
    )

    results = content.propertyCollector.RetrieveProperties([filter_spec])

    datastores = []
    for result in results:
        props = {p.name: p.val for p in result.propSet}
        if not props.get("summary.accessible", False):
            continue
        cap  = props.get("summary.capacity",  0)
        free = props.get("summary.freeSpace", 0)
        name = props.get("summary.name", "unknown")
        if cap == 0:
            continue
        datastores.append({
            "name":       name,
            "cap_bytes":  cap,
            "free_bytes": free,
        })

    return datastores


# --------------------------------------------------------------------------
# Step 6: Print table sorted by free space descending
# --------------------------------------------------------------------------
def bytes_to_tib(b):
    return b / (1024 ** 4)

def print_table(datastores, cluster_name, dc_name):
    datastores.sort(key=lambda x: x["free_bytes"], reverse=True)

    col = 50
    div = "-" * 97

    print()
    print(f"{'DATASTORE':<{col}} {'CAPACITY(TiB)':>13} {'USED(TiB)':>12} "
          f"{'FREE(TiB)':>12} {'FREE%':>8}")
    print(div)

    for ds in datastores:
        cap_tib  = bytes_to_tib(ds["cap_bytes"])
        free_tib = bytes_to_tib(ds["free_bytes"])
        used_tib = cap_tib - free_tib
        pct      = (free_tib / cap_tib * 100) if cap_tib > 0 else 0.0
        flag     = "  [!!]" if pct < 20 else ""
        print(f"{ds['name']:<{col}} {cap_tib:>13.2f} {used_tib:>12.2f} "
              f"{free_tib:>12.2f} {pct:>7.1f}%{flag}")

    print(div)
    print()
    print("  [!!] = Less than 20% free — avoid for new VM provisioning")
    print("  Sorted by free space — highest available on top")
    print()
    print(f"  Total datastores : {len(datastores)}")
    print(f"  Cluster          : {cluster_name}")
    print(f"  Datacenter       : {dc_name}")
    print()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    args = parse_args()

    missing = [(v, l) for v, l in [
        (args.url,      "GOVC_URL / --url"),
        (args.username, "GOVC_USERNAME / --username"),
        (args.password, "GOVC_PASSWORD / --password"),
    ] if not v]
    if missing:
        for _, label in missing:
            print(f"[ERROR] {label} is not set.")
        sys.exit(1)

    print()
    print(f"Connecting to : {args.url}")
    print(f"Cluster       : {args.cluster}")
    print()

    si = connect(args.url, args.username, args.password, args.no_verify_ssl)

    try:
        content = si.RetrieveContent()

        # Step 1 + 2
        print("Resolving cluster and datacenter...")
        cluster, dc = find_cluster_and_dc(content, args.cluster)
        print(f"Cluster       : {cluster.name}")
        print(f"Datacenter    : {dc.name}")
        print()

        # Step 3
        print("Fetching hosts in cluster...")
        hosts = get_hosts(cluster)
        if not hosts:
            print(f"[ERROR] No ESXi hosts found in cluster '{args.cluster}'.")
            sys.exit(1)
        print(f"Found {len(hosts)} host(s).")

        # Step 4
        print(f"Fetching datastores from datacenter '{dc.name}'...")
        ds_objects = get_datastores_in_dc(dc)
        if not ds_objects:
            print(f"[ERROR] No datastores found in datacenter '{dc.name}'.")
            sys.exit(1)
        print(f"Found {len(ds_objects)} datastore(s). Fetching capacity...")
        print()

        # Step 5
        datastores = fetch_capacities(content, ds_objects)
        if not datastores:
            print("[ERROR] Could not retrieve capacity for any datastore.")
            sys.exit(1)

        # Step 6
        print_table(datastores, cluster.name, dc.name)

    finally:
        Disconnect(si)


if __name__ == "__main__":
    main()
  
