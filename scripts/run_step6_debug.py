import logging
import sys
from pathlib import Path

# Ensure project root in path
root = Path(__file__).parent.parent
sys.path.insert(0, str(root))

from src.core.manager import GeoValidaManager

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    logger = logging.getLogger('run_step6_debug')

    manager = GeoValidaManager()

    # Load initialization data (populates graph and municipalities)
    ok = manager.load_from_initialization_json()
    if not ok:
        logger.error('Failed to load initialization.json; aborting.')
        sys.exit(1)

    # Prevent map saving (avoids matplotlib heavy operations during debugging)
    try:
        manager.map_generator.save_map = lambda *a, **k: logger.debug('save_map skipped')
        manager.map_generator.save_rm_map = lambda *a, **k: logger.debug('save_rm_map skipped')
    except Exception:
        pass

    # Inject municipalities data into SedeAnalyzer so it has metrics
    if hasattr(manager, 'municipios_data') and manager.municipios_data is not None:
        manager.sede_analyzer.df_municipios = manager.municipios_data.copy()

    # Ensure impedance data loaded
    try:
        manager.sede_analyzer.load_impedance_data()
    except Exception:
        logger.debug('Impedance load skipped or failed; continuing')

    # Run only Step 6 consolidations
    try:
        changes = manager.sede_consolidator.run_sede_consolidation(
            manager.analyzer.full_flow_df if hasattr(manager.analyzer, 'full_flow_df') else None,
            manager.map_generator.gdf_complete,
            manager.map_generator
        )
        logger.info(f'Step 6 executed, changes: {changes}')
    except Exception as e:
        logger.exception(f'Error running Step 6: {e}')

    # Export snapshot after Step 6 for inspection
    try:
        snapshot_path = Path(root) / 'data' / '03_processed' / 'snapshot_step6_sede_consolidation_debug.json'
        manager.graph.export_snapshot(snapshot_path, 'Sede Consolidation (debug run)', manager.map_generator.gdf_complete)
        logger.info(f'Snapshot exported to: {snapshot_path}')
    except Exception:
        logger.exception('Failed exporting debug snapshot')
