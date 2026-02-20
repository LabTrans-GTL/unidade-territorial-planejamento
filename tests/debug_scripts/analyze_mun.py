
import sys
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = Path.cwd()
sys.path.append(str(project_root))

from src.utils.data_loader import DataLoader

def analyze_data():
    loader = DataLoader()
    df = loader.get_municipios_dataframe()
    
    print(f"Total rows: {len(df)}")
    if 'cd_mun' in df.columns:
        print(f"Unique cd_mun: {df['cd_mun'].nunique()}")
        
        # Check for duplicates
        dupes = df[df.duplicated('cd_mun', keep=False)]
        if not dupes.empty:
            print("\nDuplicate CD_MUN found:")
            print(dupes[['cd_mun', 'nm_mun', 'uf']].sort_values('cd_mun'))
            
    # Check for invalid cd_mun (e.g. not 7 digits, or specific IDs for lakes)
    # usually lakes have code starting with 430000... or something, or names "Lagoa ..."
    
    print("\nMT Municipalities:")
    mt = df[df['uf'] == 'MT'][['cd_mun', 'nm_mun']].drop_duplicates().sort_values('nm_mun')
    for index, row in mt.iterrows():
        print(f"{row['cd_mun']} - {row['nm_mun']}")
        
    print(f"Total MT: {len(mt)}")

if __name__ == "__main__":
    analyze_data()
