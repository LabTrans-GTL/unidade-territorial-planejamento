import json
from pathlib import Path

# Verificar análise de sedes
sede_analysis = Path("data/sede_dependency_analysis.json")

if not sede_analysis.exists():
    print("ERRO: sede_dependency_analysis.json nao encontrado!")
    print("Este arquivo deveria ser gerado pelo SedeAnalyzer")
    exit(1)

with open(sede_analysis, encoding='utf-8') as f:
    analysis = json.load(f)

print("=" * 80)
print("ANALISE DE DEPENDENCIAS DAS SEDES")
print("=" * 80)

flows = analysis.get('flows', [])

# Buscar as 3 cidades
cidades = {
    2612208: "Salgueiro",
    2603009: "Cabrobó",
    2601607: "Belém do São Francisco"
}

for cd_mun, nome in cidades.items():
    flow = next((f for f in flows if f.get('cd_mun_sede') == cd_mun), None)
    
    if not flow:
        print(f"\n[ERRO] {nome} ({cd_mun}) NAO ENCONTRADO na analise!")
        continue
    
    print(f"\n{'='*80}")
    print(f"SEDE: {flow.get('nm_sede')} (CD: {cd_mun})")
    print(f"UTP: {flow.get('utp_id')}")
    print(f"{'='*80}")
    
    print(f"\nFluxo Principal:")
    print(f"  Destino: {flow.get('principal_destino_nm')} (CD: {flow.get('principal_destino_cd')})")
    print(f"  Viagens: {flow.get('viagens_destino_principal'):,}")
    print(f"  Tempo: {flow.get('tempo_ate_destino_h'):.2f}h")
    print(f"  Proporcao: {flow.get('proporcao_destino_principal')*100:.1f}%")
    
    print(f"\nAlerta de Dependencia:")
    print(f"  Tem Alerta? {flow.get('tem_alerta_dependencia')}")
    
    if flow.get('tem_alerta_dependencia'):
        print(f"  CRITICO: Esta sede tem dependencia funcional!")
    else:
        print(f"  OK: Nao ha dependencia")
    
    print(f"\nMetricas Socieconomicas:")
    print(f"  Aeroporto: {flow.get('tem_aeroporto')}")
    print(f"  ICAO: {flow.get('aeroporto_icao')}")
    print(f"  Turismo: {flow.get('turismo')}")
    print(f"  REGIC: {flow.get('regic')}")
