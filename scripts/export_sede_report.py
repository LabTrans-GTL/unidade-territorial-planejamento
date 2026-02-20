
import json
import pandas as pd
from pathlib import Path

def export_sede_report():
    project_root = Path(__file__).parent.parent
    log_path = project_root / "data" / "consolidation_log.json"
    init_data_path = project_root / "data" / "initialization.json"
    output_path = project_root / "data" / "sede_consolidation_report.csv"

    print(f"Loading log from {log_path}...")
    if not log_path.exists():
        print("Log file not found.")
        return

    with open(log_path, 'r', encoding='utf-8') as f:
        log_data = json.load(f)

    # Load names mapping
    print(f"Loading metadata from {init_data_path}...")
    mun_names = {}
    if init_data_path.exists():
        with open(init_data_path, 'r', encoding='utf-8') as f:
            init_data = json.load(f)
            for m in init_data.get('municipios', []):
                mun_names[int(m['cd_mun'])] = m.get('nm_mun', str(m['cd_mun']))
    
    # Filter and Process
    rows = []
    for entry in log_data.get('consolidations', []):
        reason = entry.get('reason', '')
        details = entry.get('details', {})
        
        # Check if it looks like a Sede Consolidation Rule (starts with 0/ or contains REGIC priority from Step 6)
        # SedeConsolidator uses reasons like "0/0: ...", "0/1: ...", "0/2: ..."
        # It also has "scores" in details.
        
        is_sede_rule = False
        if reason.startswith("0/"):
            is_sede_rule = True
        elif "REGIC Priority" in reason and "scores" in details:
             is_sede_rule = True
             
        if is_sede_rule:
            mun_id = details.get('mun_id')
            mun_name = mun_names.get(int(mun_id), str(mun_id)) if mun_id else "Unknown"
            
            # Identify if it was a Sede moving (most important for user)
            is_sede_migration = details.get('sede_migration', False)
            
            rows.append({
                "Municipio_ID": mun_id,
                "Municipio_Nome": mun_name,
                "UTP_Origem": entry.get('source_utp'),
                "UTP_Destino": entry.get('target_utp'),
                "Motivo": reason,
                "Pontuacao_Scores": details.get('scores', ''),
                "E_Sede_Original": is_sede_migration,
                "Timestamp": entry.get('timestamp')
            })

    if not rows:
        print("No sede consolidations found in log.")
        return

    df = pd.DataFrame(rows)
    print(f"Found {len(df)} consolidation records.")
    
    # Save
    df.to_csv(output_path, index=False, encoding='utf-8-sig') # sig for excel compatibility
    print(f"Report saved to: {output_path}")

if __name__ == "__main__":
    export_sede_report()
