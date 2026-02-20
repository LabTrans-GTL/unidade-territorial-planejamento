
import logging
import sys
import os
import pandas as pd
from pathlib import Path

# Add project root to path
sys.path.append(os.getcwd())
from src.pipeline.sede_analyzer import SedeAnalyzer

def inspect():
    analyzer = SedeAnalyzer()
    if not analyzer.load_initialization_data():
        print("Failed to load data")
        return

    df = analyzer.df_municipios
    if 'regic' not in df.columns:
        print("Column 'regic' not found in df_municipios")
        # Check if it's nested in some other dict or if we're filtering out
        # Actually SedeAnalyzer extracts it in metrics. Let's see raw.
        print(df.columns)
        return

    # Check unique values
    unique_regic = df['regic'].unique()
    print("Unique REGIC values:", unique_regic)
    
    # Check type
    print("Type:", df['regic'].dtype)
    
    # Print a few examples with names
    print(df[['nm_mun', 'regic']].head(10))

if __name__ == "__main__":
    inspect()
