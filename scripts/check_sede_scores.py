
import logging
import sys
import os
sys.path.append(os.getcwd())
from src.core.manager import GeoValidaManager

def check_scores():
    # Setup
    manager = GeoValidaManager()
    # FORCE SILENCE
    logging.getLogger().handlers = []
    logging.getLogger('GeoValida').handlers = []
    logging.getLogger('GeoValida').propagate = False
    manager.step_0_initialize_data()
    
    # Load Shapefiles for fallback
    if manager.map_generator.gdf_complete is None or manager.map_generator.gdf_complete.empty:
         manager.map_generator.load_shapefiles()

    # Setup analyzer
    manager.sede_analyzer.load_initialization_data()
    manager.sede_analyzer.load_impedance_data()
    
    # Calculate metrics
    print("Calculating metrics...")
    df_metrics = manager.sede_analyzer.calculate_socioeconomic_metrics()
    
    # IDs
    # Salgueiro: 2612208
    # Cabrobó: 2603009
    # Belém do São Francisco: 2601607
    
    target_ids = [2612208, 2603009, 2601607]
    
    for mid in target_ids:
        row = df_metrics[df_metrics['cd_mun_sede'] == mid]
        if row.empty:
            print(f"ID {mid} not found in sedes.")
            # Check if it exists in raw data
            raw = manager.sede_analyzer.df_municipios[manager.sede_analyzer.df_municipios['cd_mun'] == mid]
            if not raw.empty:
                print(f"  Found in raw data. Sede_UTP: {raw.iloc[0].get('sede_utp')}")
            else:
                print("  Not found in raw data either.")
            continue
            
        row = row.iloc[0]
        print(f"\n--- {row['nm_sede']} ({mid}) ---")
        print(f"UTP: {row['utp_id']}")
        print(f"Tem Aeroporto: {row['tem_aeroporto']} (ICAO: {row.get('aeroporto_icao')})")
        print(f"Turismo: '{row['turismo']}'")
        
        # Calculate Score manually as done in Consolidator
        score = 0
        if row['tem_aeroporto']: score += 1
        
        turismo = str(row.get('turismo', '')).upper()
        if turismo == "1 - Municipio Turistico": 
             score += 1
             
        print(f"Calculated Score: {score}")
        
        # Check Flow
        print(f"Principal Destino: {row['principal_destino_nm']} ({row['principal_destino_cd']})")
        print(f"Tempo: {row['tempo_ate_destino_h']}")
        print(f"Alerta: {row['tem_alerta_dependencia']}")

if __name__ == "__main__":
    check_scores()
