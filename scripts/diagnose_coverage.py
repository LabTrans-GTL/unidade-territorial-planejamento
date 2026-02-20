
import pandas as pd
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Diagnose")

def diagnose():
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "data"
    
    # Files
    od_path = data_dir / "01_raw" / "person-matrix-data" / "base_dados_rodoviaria_coletiva_2023.csv"
    imp_path = data_dir / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
    
    # 1. Load OD (sample or full?) 
    # Let's load full but only necessary columns
    logger.info("📦 Loading OD Data...")
    try:
        df_od = pd.read_csv(od_path, sep=';', usecols=['mun_origem', 'mun_destino', 'viagens'])
        logger.info(f"   OD Loaded: {len(df_od)} rows")
    except:
        # Fallback 
        df_od = pd.read_csv(od_path, sep=',', usecols=['mun_origem', 'mun_destino', 'viagens'])
    
    # Ensure types
    df_od['mun_origem'] = pd.to_numeric(df_od['mun_origem'], errors='coerce')
    df_od['mun_destino'] = pd.to_numeric(df_od['mun_destino'], errors='coerce')
    df_od = df_od.dropna()
    df_od['mun_origem'] = df_od['mun_origem'].astype(int)
    df_od['mun_destino'] = df_od['mun_destino'].astype(int)
    
    # 2. Get Top 1 Destination for each Origin
    logger.info("🔄 Identifying Top 1 Destination per Municipality...")
    # Sort by viagens desc
    df_od_sorted = df_od.sort_values('viagens', ascending=False)
    # Drop duplicates keeps first (which is max)
    top_flows = df_od_sorted.drop_duplicates('mun_origem')[['mun_origem', 'mun_destino', 'viagens']]
    
    total_muns = len(top_flows)
    logger.info(f"   Analyzed {total_muns} unique origins")
    
    # 3. Load Impedance
    logger.info("📦 Loading Impedance Data...")
    try:
        df_imp = pd.read_csv(imp_path, sep=';', decimal=',', usecols=['origem', 'destino', 'tempo'])
    except:
        # If headers are different, try index
         df_imp = pd.read_csv(imp_path, sep=';', decimal=',')
         df_imp.columns = ['par_ibge', 'origem', 'destino', 'tempo']
    
    df_imp['origem'] = pd.to_numeric(df_imp['origem'], errors='coerce').fillna(0).astype(int)
    df_imp['destino'] = pd.to_numeric(df_imp['destino'], errors='coerce').fillna(0).astype(int)
    
    # Create set of available pairs (normalize to 6 digits)
    logger.info("   Creating lookup set (with 6-digit normalization)...")
    # Tuple (orig // 10, dest // 10)
    available_pairs = set(zip(
        df_imp['origem'] // 10, 
        df_imp['destino'] // 10
    ))
    logger.info(f"   Impedance data has {len(available_pairs)} unique 6-digit pairs (<= 2h)")
    
    # 4. Check Match Rate
    logger.info("🔍 Checking Coverage...")
    
    matches = 0
    missing = 0
    
    missing_samples = []
    
    for idx, row in top_flows.iterrows():
        orig = int(row['mun_origem']) // 10
        dest = int(row['mun_destino']) // 10
        
        if (orig, dest) in available_pairs:
            matches += 1
        else:
            missing += 1
            if len(missing_samples) < 10:
                missing_samples.append((int(row['mun_origem']), int(row['mun_destino']), row['viagens']))
    
    logger.info("="*40)
    logger.info("RESULTS")
    logger.info("="*40)
    logger.info(f"Total Municipalities in OD: {total_muns}")
    logger.info(f"Municipalities whose Top 1 Dest is in Impedance (<= 2h): {matches}")
    logger.info(f"Coverage: {matches / total_muns * 100:.2f}%")
    logger.info("="*40)
    
    if missing_samples:
        logger.info("Sample of Top Flows NOT in Impedance (likely > 2h):")
        for o, d, v in missing_samples:
             logger.info(f"   Origin: {o} -> Dest: {d} (Viagens: {v})")

if __name__ == "__main__":
    diagnose()
