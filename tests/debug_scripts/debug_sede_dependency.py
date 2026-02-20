"""
Script para debug de dependencias de sedes especificas
"""
from src.core.manager import GeoValidaManager
import pandas as pd

print("Inicializando manager...")
m = GeoValidaManager()

print("Carregando dados...")
m.step_0_initialize_data()

print("Analisando fluxos...")
m.step_2_analyze_flows()

print("\nCriando SedeAnalyzer...")
from src.pipeline.sede_analyzer import SedeAnalyzer
from pathlib import Path

# SedeAnalyzer procura initialization.json no data_path
# Deve ser o diretório raiz de dados
data_dir = Path("data")
sede_analyzer = SedeAnalyzer(str(data_dir))

# Carregar dados explicitamente (o __init__ não faz isso automaticamente)
print("Carregando initialization.json...")
if not sede_analyzer.load_initialization_data():
    print("[ERRO] Falha ao carregar initialization.json!")
    exit(1)

print("Carregando impedancias...")
if not sede_analyzer.load_impedance_data():
    print("[AVISO] Falha ao carregar impedancias (pode usar fallback)")

print(f"\nDados carregados pelo SedeAnalyzer:")
print(f"  - Municipios: {len(sede_analyzer.df_municipios) if sede_analyzer.df_municipios is not None else 0}")
print(f"  - Impedancias: {len(sede_analyzer.df_impedance) if sede_analyzer.df_impedance is not None else 0}")

print("\nCalculando metricas socioeconomicas...")
df_metrics = sede_analyzer.calculate_socioeconomic_metrics()

print(f"Total de sedes analisadas: {len(df_metrics)}")
print(f"Colunas: {list(df_metrics.columns)}")

# Cidades para verificar
cidades = {
    2612208: "Salgueiro",
    2603009: "Cabrobó",
    2601607: "Belém do São Francisco"
}

print("\n" + "=" * 80)
print("ANALISE DE DEPENDENCIA")
print("=" * 80)

for cd_mun, nome in cidades.items():
    row = df_metrics[df_metrics['cd_mun_sede'] == cd_mun]
    
    if row.empty:
        print(f"\n[ERRO] {nome} ({cd_mun}) NAO ENCONTRADO nas metricas!")
        continue
    
    row = row.iloc[0]
    
    print(f"\n{'-' * 80}")
    print(f"SEDE: {row['nm_sede']} (CD: {cd_mun})")
    print(f"UTP: {row['utp_id']}")
    print(f"{'-' * 80}")
    
    print(f"\nFluxo Principal:")
    print(f"  Destino: {row.get('principal_destino_nm', 'N/A')} (CD: {row.get('principal_destino_cd', 'N/A')})")
    print(f"  Viagens: {row.get('viagens_destino_principal', 0):,}")
    print(f"  Tempo: {row.get('tempo_ate_destino_h', 'N/A')}")
    print(f"  Proporcao: {row.get('proporcao_destino_principal', 0)*100:.1f}%")
    
    print(f"\nAlerta de Dependencia:")
    tem_alerta = row.get('tem_alerta_dependencia', False)
    print(f"  Status: {'SIM - DETECTADO!' if tem_alerta else 'NAO - Nenhum alerta'}")
    
    if not tem_alerta:
        print(f"\n  [DIAGNOSTICO] Por que nao tem alerta?")
        
        # Verificar criterios
        dest_cd = row.get('principal_destino_cd')
        tempo = row.get('tempo_ate_destino_h')
        
        if pd.isna(dest_cd):
            print(f"    - Sem destino principal identificado")
        else:
            # Verificar se destino e sede
            dest_row = df_metrics[df_metrics['cd_mun_sede'] == dest_cd]
            if dest_row.empty:
                print(f"    - Destino ({dest_cd}) NAO E SEDE!")
            else:
                print(f"    - Destino e sede: OK")
            
            if pd.isna(tempo):
                print(f"    - Tempo de viagem: NAO DISPONIVEL (sem dados de impedancia)")
            elif tempo > 2.0:
                print(f"    - Tempo de viagem: {tempo:.2f}h > 2.0h (REJEITADO)")
            else:
                print(f"    - Tempo de viagem: {tempo:.2f}h <= 2.0h (OK)")
    
    print(f"\nInfraestrutura:")
    print(f"  Aeroporto: {row.get('tem_aeroporto', False)}")
    print(f"  ICAO: {row.get('aeroporto_icao', 'N/A')}")
    print(f"  Turismo: {row.get('turismo', 'N/A')}")
    print(f"  REGIC: {row.get('regic', 'N/A')}")

print("\n" + "=" * 80)
