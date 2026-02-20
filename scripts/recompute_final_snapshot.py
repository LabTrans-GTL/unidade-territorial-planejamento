from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.manager import GeoValidaManager

if __name__ == '__main__':
    m = GeoValidaManager()
    print('Initializing data...')
    m.step_0_initialize_data()
    print('Syncing map with graph...')
    m.map_generator.sync_with_graph(m.graph)
    print('Computing coloring...')
    gdf = m.map_generator.gdf_complete
    snapshot_path = Path('data/03_processed/snapshot_step8_final.json')
    if gdf is not None and not gdf.empty:
        if 'utp_id' in gdf.columns and 'UTP_ID' not in gdf.columns:
            gdf['UTP_ID'] = gdf['utp_id']
        coloring = m.graph.compute_graph_coloring(gdf)
        gdf['COLOR_ID'] = gdf['CD_MUN'].astype(int).map(coloring).fillna(0).astype(int)
    print('Exporting snapshot...')
    m.graph.export_snapshot(snapshot_path, 'Border Validation + Isolated Resolution (recompute)', m.map_generator.gdf_complete)
    print('Done')
