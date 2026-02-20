from pathlib import Path
from src.core.graph import TerritorialGraph
from src.interface.consolidation_manager import ConsolidationManager
import json
import pandas as pd

DATA_DIR = Path(__file__).parent.parent / 'data' / '03_processed'
SNAPSHOT_IN = DATA_DIR / 'snapshot_step1_initial.json'
SNAPSHOT_OUT = DATA_DIR / 'snapshot_step6_sede_consolidation.json'
CSV_OUT = DATA_DIR / 'sede_consolidation_result.csv'
LOG_PATH = Path(__file__).parent.parent / 'data' / 'consolidation_log.json'

TO_MOVE = [2601607, 2603009]
TARGET_UTP = '675'
SEED_MUN = 2612208

print('Loading graph snapshot...')
G = TerritorialGraph()
G.load_snapshot(SNAPSHOT_IN)

con_manager = ConsolidationManager()
new_consolidations = []

for mun in TO_MOVE:
    orig_utp = G.get_municipality_utp(mun)
    print(f'Municipio {mun} original UTP: {orig_utp}')
    if str(orig_utp) == str(TARGET_UTP):
        print(f'  Already in target UTP {TARGET_UTP}, skipping')
        continue
    try:
        G.move_municipality(mun, TARGET_UTP)
        print(f'  Moved {mun} -> UTP_{TARGET_UTP}')
    except Exception as e:
        print(f'  Error moving {mun}: {e}')
    # Build consolidation entry
    cons = con_manager.add_consolidation(
        source_utp=str(orig_utp),
        target_utp=str(TARGET_UTP),
        reason='Manual force: ensure Salgueiro UTP',
        details={
            'sede_id': mun,
            'sede_destino': SEED_MUN,
            'is_sede': True,
            'nm_sede': '',
            'municipalities_moved': 1,
            'score_origem': '',
            'score_destino': '',
            'tempo_viagem_h': '',
            'rm_origem': '',
            'rm_destino': '',
            'transitive': False,
            'transitive_reason': ''
        }
    )
    new_consolidations.append(cons)

# Ensure seed mapping for target UTP
G.utp_seeds[str(TARGET_UTP)] = SEED_MUN

# Export snapshot
print('Exporting snapshot...')
G.export_snapshot(SNAPSHOT_OUT, 'Forced Sede Consolidation', None)

# Save sede result via ConsolidationManager
print('Saving sede result (json)...')
con_manager.save_sede_batch(new_consolidations)

# Save/overwrite CSV for these entries (append to existing or create new)
records = []
for cons in new_consolidations:
    details = cons.get('details', {})
    records.append({
        'sede_origem': details.get('sede_id', ''),
        'utp_origem': cons.get('source_utp', ''),
        'sede_destino': details.get('sede_destino', ''),
        'utp_destino': cons.get('target_utp', ''),
        'tempo_viagem_h': details.get('tempo_viagem_h', ''),
        'score_origem': details.get('score_origem', ''),
        'score_destino': details.get('score_destino', ''),
        'rm_origem': details.get('rm_origem', ''),
        'rm_destino': details.get('rm_destino', ''),
        'status': 'APROVADO',
        'transitive': 'SIM' if details.get('transitive', False) else 'NAO',
        'transitive_reason': details.get('transitive_reason', ''),
        'motivo_rejeicao': ''
    })

if records:
    df = pd.DataFrame(records)
    if CSV_OUT.exists():
        try:
            df_old = pd.read_csv(CSV_OUT)
            df_new = pd.concat([df_old, df], ignore_index=True)
            df_new.to_csv(CSV_OUT, index=False, encoding='utf-8-sig')
        except Exception:
            df.to_csv(CSV_OUT, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(CSV_OUT, index=False, encoding='utf-8-sig')

print('Done. Updated snapshot and CSV.')
