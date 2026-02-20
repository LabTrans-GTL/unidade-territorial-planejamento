
import pandas as pd
from pathlib import Path
import os

def check_pairs():
    file_path = Path("data/01_raw/impedance/impedancias_filtradas_2h.csv")
    print(f"Checking {file_path.absolute()}")
    
    if not file_path.exists():
        print("File not found!")
        return

    # Try reading first few lines to detect separator
    with open(file_path, 'r', encoding='utf-8') as f:
        print("First 3 lines:")
        for _ in range(3):
            print(f.readline().strip())
            
    # Load full
    try:
        df = pd.read_csv(file_path, sep=';', dtype=str)
        print(f"Loaded {len(df)} rows.")
        print("Columns:", df.columns.tolist())
        
        # Pairs to check
        # Salgueiro: 2612208
        # Cabrobó: 2603009
        # Belém: 2601607
        
        pairs = [
            ('2603009', '2612208'), # Cabrobó -> Salgueiro
            ('2612208', '2603009'), # Salgueiro -> Cabrobó
            ('2601607', '2603009'), # Belém -> Cabrobó
            ('2603009', '2601607')  # Cabrobó -> Belém
        ]
        
        # Standardization
        # Assuming cols are roughly: [pair_id, origin, dest, time]
        # Based on pipeline code: ['par_ibge', 'origem', 'destino', 'tempo_horas']
        # But let's check actual column names
        
        col_orig = df.columns[1] # Guessing index 1 based on pipelines
        col_dest = df.columns[2]
        
        print(f"Using Origin Col: {col_orig}, Dest Col: {col_dest}")
        
        for o, d in pairs:
            print(f"\nSearching for {o} -> {d}")
            match = df[(df[col_orig] == o) & (df[col_dest] == d)]
            if not match.empty:
                print("FOUND!")
                print(match)
            else:
                print("NOT FOUND.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_pairs()
