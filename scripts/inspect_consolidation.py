
import pandas as pd
import json
from pathlib import Path

# Path to the CSV file
data_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed")
csv_path = data_dir / "sede_consolidation_result.csv"

# Municipality code to investigate
target_sede = 4316808

print(f"Investigating sede consolidation for: {target_sede}")

if not csv_path.exists():
    print(f"Error: CSV file not found at {csv_path}")
    exit(1)

try:
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} records from {csv_path.name}")
    
    # Check for the target sede in 'sede_origem'
    # Currently csv has 'sede_origem' as float/int, ensure matching type
    # It might be read as int or float depending on NaNs
    
    # Filter for the target
    # We use string comparison for safety
    target_rows = df[df['sede_origem'].astype(str).str.contains(str(target_sede))]
    
    if target_rows.empty:
        print(f"No records found for sede {target_sede} in the consolidation results.")
    else:
        print(f"Found {len(target_rows)} records for {target_sede}:")
        for _, row in target_rows.iterrows():
            print("\n--- Record ---")
            for col in df.columns:
                print(f"{col}: {row[col]}")
                
    # Also check other potential issues: High score sedes being consolidated?
    # Filter for approved consolidations where score_origem is high (e.g., >= 2)
    print("\n--- Checking for high-score sedes that were consolidated (Score >= 2) ---")
    high_score_consolidated = df[
        (df['status'] == 'APROVADO') & 
        (df['score_origem'] >= 2)
    ]
    
    if not high_score_consolidated.empty:
        print(f"Found {len(high_score_consolidated)} high-score sedes consolidated:")
        print(high_score_consolidated[['sede_origem', 'utp_origem', 'utp_destino', 'score_origem', 'score_destino', 'tempo_viagem_h']].to_string())
    else:
        print("No high-score sedes (>=2) were consolidated.")

except Exception as e:
    print(f"An error occurred: {e}")
