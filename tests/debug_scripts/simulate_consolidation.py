"""
Script to simulate Sede Consolidation for Belém (2601607)
"""
import logging
from src.core.manager import GeoValidaManager
from src.pipeline.sede_consolidator import SedeConsolidator

# Configure logging to stdout
logging.basicConfig(level=logging.INFO)

print("Initializing Manager...")
m = GeoValidaManager()
m.step_0_initialize_data()

# Ensure SedeAnalyzer has map generator
if not m.sede_analyzer.map_generator:
    print("Injecting MapGenerator into Analyzer...")
    m.sede_analyzer.map_generator = m.map_generator

# Explicitly load data for Analyzer (since we are skipping full pipeline steps)
print("Loading data for SedeAnalyzer...")
m.sede_analyzer.load_initialization_data()
m.sede_analyzer.load_impedance_data()

print("\n--- Running Sede Consolidation Simulation ---")
consolidator = m.sede_consolidator

# Force calculation of metrics
print("Calculating metrics...")
df_metrics = m.sede_analyzer.calculate_socioeconomic_metrics()

# Check alert for Belém
belem_row = df_metrics[df_metrics['cd_mun_sede'] == 2601607]
print("\n[DEBUG] Belém Alert Status:")
print(belem_row[['nm_sede', 'tem_alerta_dependencia', 'principal_destino_nm', 'tempo_ate_destino_h', 'alerta_detalhes']].iloc[0])

# Run filter candidates manually
print("\n[DEBUG] Filtering Candidates...")
candidates = consolidator._filter_candidates(df_metrics, iteration=1)

# Check if Belém is in candidates
belem_cand = next((c for c in candidates if c['sede_origem'] == 2601607), None)
if belem_cand:
    print("\n✅ Belém ACCEPTED as candidate:")
    print(belem_cand)
else:
    print("\n❌ Belém REJECTED. Why?")
    # Re-run logic for Belém to see rejection reason
    # ... logic copied from SedeConsolidator or inferred from logs if run via consolidator ...
    
    # Let's verify the scores manually
    belem_score = consolidator._get_sede_score(belem_row.iloc[0])
    
    # Get Destino
    dest_cd = belem_row.iloc[0]['principal_destino_cd']
    dest_row = df_metrics[df_metrics['cd_mun_sede'] == dest_cd]
    
    if not dest_row.empty:
        dest_score = consolidator._get_sede_score(dest_row.iloc[0])
        print(f"  Scores: Belém={belem_score} -> Dest={dest_score}")
        
        # Check Transitive
        target_dest_cd = dest_row.iloc[0]['principal_destino_cd']
        print(f"  Destino ({dest_row.iloc[0]['nm_sede']}) points to: {dest_row.iloc[0]['principal_destino_nm']} ({target_dest_cd})")
        print(f"  Destino Alert? {dest_row.iloc[0]['tem_alerta_dependencia']}")
        
        if dest_row.iloc[0]['tem_alerta_dependencia']:
             ult_row = df_metrics[df_metrics['cd_mun_sede'] == target_dest_cd]
             if not ult_row.empty:
                 ult_score = consolidator._get_sede_score(ult_row.iloc[0])
                 print(f"  Ultimate Dest Score: {ult_score}")

print("\n--- Simulation End ---")
