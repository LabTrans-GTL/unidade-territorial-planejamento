"""
Script de teste para verificar se SedeAnalyzer detecta dependências corretamente
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils import DataLoader
from src.pipeline.sede_analyzer import SedeAnalyzer

# Carregar dados
print("Carregando dados...")
data_path = Path(__file__).parent / "data" / "03_processed"

# Criar analyzer com caminho
analyzer = SedeAnalyzer(str(data_path))

# Carregar municipios para usar no teste
df_municipios = analyzer.df_municipios
print(f"Total de municipios: {len(df_municipios)}")

# Cidades para testar
cidades = {
    2612208: "Salgueiro",
    2603009: "Cabrobó",
    2601607: "Belém do São Francisco"
}

print("\n" + "=" * 80)
print("TESTE DE DETECCAO DE DEPENDENCIA")
print("=" * 80)

for cd_mun, nome in cidades.items():
    print(f"\n{'-' * 80}")
    print(f"Testando: {nome} (CD: {cd_mun})")
    print(f"{'-' * 80}")
    
    # Verificar se é sede
    is_sede = analyzer.is_sede(cd_mun)
    print(f"  E sede? {is_sede}")
    
    if not is_sede:
        print(f"  [SKIP] Nao e sede, pulando teste...")
        continue
    
    # Obter principal destino
    cd_destino, proporcao, total, viagens = analyzer.get_main_flow_destination(cd_mun)
    
    if cd_destino is None:
        print(f"  [AVISO] Sem dados de fluxo!")
        continue
    
    print(f"\n  Fluxo Principal:")
    print(f"    Destino CD: {cd_destino}")
    print(f"    Proporcao: {proporcao*100:.1f}%")
    print(f"    Total viagens: {total:,}")
    print(f"    Viagens para destino: {viagens:,}")
    
    # Verificar se destino é sede
    dest_is_sede = analyzer.is_sede(cd_destino)
    print(f"\n  Destino e sede? {dest_is_sede}")
    
    if not dest_is_sede:
        print(f"  [CRITERIO NAO ATENDIDO] Destino nao e sede!")
        continue
    
    # Nome do destino
    dest_mun = df_municipios[df_municipios['cd_mun'] == cd_destino]
    if not dest_mun.empty:
        print(f"  Nome destino: {dest_mun.iloc[0]['nm_mun']}")
    
    # Verificar tempo
    tempo = analyzer.get_travel_time(cd_mun, cd_destino)
    print(f"\n  Tempo de viagem: {tempo if tempo else 'N/A'}")
    
    if tempo is None:
        print(f"  [CRITERIO NAO ATENDIDO] Sem dados de tempo!")
        continue
    
    if tempo > 2.0:
        print(f"  [CRITERIO NAO ATENDIDO] Tempo > 2h (tem {tempo:.2f}h)")
        continue
    
    # Chamar check_dependency_criteria
    print(f"\n  Chamando check_dependency_criteria()...")
    alerta = analyzer.check_dependency_criteria(cd_mun)
    
    if alerta:
        print(f"  [SUCESSO] ALERTA DETECTADO!")
        print(f"    {alerta.get('alerta')}")
        print(f"    Origem: {alerta.get('nm_sede_origem')} (UTP {alerta.get('utp_origem')})")
        print(f"    Destino: {alerta.get('nm_sede_destino')} (UTP {alerta.get('utp_destino')})")
    else:
        print(f"  [FALHA] NENHUM ALERTA DETECTADO!")

print("\n" + "=" * 80)
print("FIM DO TESTE")
print("=" * 80)
