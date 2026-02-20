
import pandas as pd
from pathlib import Path
import sys

# Setup logging
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Debug")

def check_data():
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    
    # 1. Load OD Sample
    od_path = data_dir / "01_raw" / "person-matrix-data" / "base_dados_rodoviaria_coletiva_2023.csv"
    logger.info(f"Checking OD file: {od_path}")
    
    if not od_path.exists():
        logger.error("OD file not found")
        return

    # Try reading a few lines
    df_od = pd.read_csv(od_path, sep=';', nrows=100) # Trying ; first as common in Brazil data
    if len(df_od.columns) <= 1:
        df_od = pd.read_csv(od_path, sep=',', nrows=100)
    
    logger.info("OD columns: " + str(df_od.columns.tolist()))
    logger.info(f"OD dtypes:\n{df_od.dtypes}")
    
    if 'CD_MUN_ORIGEM' in df_od.columns:
        od_col = 'CD_MUN_ORIGEM' 
        dest_col = 'CD_MUN_DESTINO'
    elif 'mun_origem' in df_od.columns:
        od_col = 'mun_origem'
        dest_col = 'mun_destino'
    else:
        # Lowercase check
        df_od.columns = df_od.columns.str.lower()
        if 'mun_origem' in df_od.columns:
            od_col = 'mun_origem'
            dest_col = 'mun_destino'
        else:
            logger.error("Could not find origin/dest columns in OD")
            return

    # Check sample values
    logger.info(f"OD Sample Origin values: {df_od[od_col].head().tolist()}")
    
    # 2. Load Impedance Sample
    imp_path = data_dir / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
    logger.info(f"Checking Impedance file: {imp_path}")
    
    if not imp_path.exists():
        logger.error("Impedance file not found")
        return

    df_imp = pd.read_csv(imp_path, sep=';', decimal=',', nrows=100)
    logger.info("Impedance columns: " + str(df_imp.columns.tolist()))
    
    # Fix columns
    if len(df_imp.columns) >= 4:
        df_imp.columns = ['par_ibge', 'origem', 'destino', 'tempo']
    
    logger.info(f"Impedance dtypes:\n{df_imp.dtypes}")
    logger.info(f"Impedance Sample Origin values: {df_imp['origem'].head().tolist()}")

    # 3. Simulate Merge
    # Ensure types match
    try:
        df_od[od_col] = df_od[od_col].astype(int)
        df_imp['origem'] = df_imp['origem'].astype(int)
        
        logger.info("Converted OD origin to int successfully")
    except Exception as e:
        logger.error(f"Failed to convert OD origin to int: {e}")
    
    # Check intersection
    common_origins = set(df_od[od_col]).intersection(set(df_imp['origem']))
    logger.info(f"Common origins in sample: {len(common_origins)}")
    
    if not common_origins:
        logger.warning(f"No common origins found in first 100 rows!")
        logger.info(f"OD Sample: {df_od[od_col].unique()}")
        logger.info(f"Imp Sample: {df_imp['origem'].unique()}")

if __name__ == "__main__":
    check_data()
