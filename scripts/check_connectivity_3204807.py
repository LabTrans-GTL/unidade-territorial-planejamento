#!/usr/bin/env python3
import json
import sys
from pathlib import Path

import geopandas as gpd

# Ensure project root is on sys.path so 'src' package can be imported
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline.border_validator_v2 import BorderValidatorV2


def main():
    data_dir = Path(__file__).parent.parent / 'data'
    geo_path = data_dir / '04_maps' / 'municipalities_optimized.geojson'
    snap_path = data_dir / '03_processed' / 'snapshot_step8_border_validation.json'
    mun = 3204807

    bv = BorderValidatorV2(graph=None, validator=None)

    if not geo_path.exists():
        print("geojson missing:", geo_path)
        return

    gdf = gpd.read_file(geo_path)
    print("GeoDataFrame loaded, rows:", len(gdf))

    bv._build_adjacency_graph(gdf)
    G = bv.adjacency_graph
    print("Adjacency graph: nodes=", G.number_of_nodes(), "edges=", G.number_of_edges())

    if mun in G:
        neigh = list(G.neighbors(mun))
    else:
        neigh = []
    print(f"Neighbors of {mun}:", neigh)

    if not snap_path.exists():
        print("snapshot missing:", snap_path)
        return

    snap = json.load(open(snap_path, 'r', encoding='utf-8'))

    # Parse nodes: snapshot['nodes'] is a dict keyed by node id (e.g., '3204807')
    nodes = snap.get('nodes')
    if not nodes or not isinstance(nodes, dict):
        print("Could not parse nodes from snapshot (unexpected structure)")
        return

    utp_map = {}
    # Also collect utp -> seed mapping from node attributes as fallback
    utp_seeds_from_nodes = {}
    for key, val in nodes.items():
        try:
            node_type = val.get('type')
            if node_type == 'municipality':
                cid = int(key)
                utp = val.get('utp_id') or val.get('UTP_ID') or val.get('utp')
                if utp is not None:
                    utp_map[cid] = str(utp)
                # If this municipality is marked as sede, register as seed
                if val.get('sede_utp'):
                    utp_seeds_from_nodes[str(utp_map.get(cid, ''))] = cid
        except Exception:
            continue

    mun_utp = utp_map.get(mun)
    print(f"Municipality {mun} UTP (snapshot):", mun_utp)

    # utp seeds
    utp_seeds = snap.get('utp_seeds') or snap.get('seeds') or {}
    sede = None
    if mun_utp and str(mun_utp) in utp_seeds:
        try:
            sede = int(utp_seeds[str(mun_utp)])
        except Exception:
            sede = None

    if sede is None:
        # fallback: find node marked as sede
        for n in nodes:
            try:
                if (n.get('utp_id') == mun_utp or n.get('UTP_ID') == mun_utp) and n.get('sede_utp'):
                    sede = int(n.get('cd_mun'))
                    break
            except Exception:
                continue

    print(f"Sede for UTP {mun_utp}:", sede)

    utp_muns = [m for m, u in utp_map.items() if u == mun_utp]
    print(f"Total municipalities in UTP {mun_utp}:", len(utp_muns))

    connected = bv._is_connected_to_sede_within_utp(mun, sede, utp_muns)
    print(f"Connected to sede within UTP? {connected}")


if __name__ == '__main__':
    main()
