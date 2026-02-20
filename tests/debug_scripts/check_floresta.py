"""
Script para diagnosticar o caso específico de Floresta (2605707).
Verifica por que o município não está sendo movido para UTP 321.
"""
import sys
import json
from pathlib import Path
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.graph import TerritorialGraph

# Código IBGE de Floresta
FLORESTA_ID = 2605707
EXPECTED_UTP = "321"

print("=" * 80)
print(f"DIAGNÓSTICO: Floresta (IBGE {FLORESTA_ID})")
print("=" * 80)

# 1. Carregar o graph do último snapshot
snapshot_path = Path("data/03_processed/snapshot_step8_border_validation.json")

if not snapshot_path.exists():
    print(f"\n[ERRO] Snapshot não encontrado: {snapshot_path}")
    print("Execute o pipeline primeiro!")
    sys.exit(1)

with open(snapshot_path, encoding='utf-8') as f:
    snapshot = json.load(f)

# Reconstruir o grafo
print("\n[1] Carregando grafo...")
graph = TerritorialGraph()

# Carregar hierarchia do snapshot
if 'hierarchy' in snapshot:
    for node_str, node_data in snapshot['hierarchy']['nodes'].items():
        node_id = int(node_str) if node_data['type'] in ['municipality', 'sede'] else node_str
        graph.hierarchy.add_node(node_id, **node_data)
    
    for edge in snapshot['hierarchy']['edges']:
        graph.hierarchy.add_edge(edge['source'], edge['target'])

# Verificar se Floresta existe
if FLORESTA_ID not in graph.hierarchy.nodes:
    print(f"\n[ERRO] Floresta ({FLORESTA_ID}) não encontrado no grafo!")
    sys.exit(1)

# 2. Estado atual de Floresta
floresta_data = graph.hierarchy.nodes[FLORESTA_ID]
current_utp = graph.get_municipality_utp(FLORESTA_ID)

print(f"\n[2] Estado atual de Floresta:")
print(f"   Nome: {floresta_data.get('name', 'N/A')}")
print(f"   UTP Atual: {current_utp}")
print(f"   UTP Esperada: {EXPECTED_UTP}")
print(f"   É Sede: {floresta_data.get('sede_utp', False)}")
print(f"   RM: {floresta_data.get('regiao_metropolitana', 'SEM_RM')}")

# 3. Carregar dados de fluxo
print(f"\n[3] Carregando dados de fluxo...")
init_path = Path("data/initialization.json")

if not init_path.exists():
    print(f"   [ERRO] initialization.json não encontrado!")
    sys.exit(1)

with open(init_path, encoding='utf-8') as f:
    init_data = json.load(f)

# Encontrar Floresta nos dados de inicialização
floresta_init = next((m for m in init_data.get('municipios', []) if m['cd_mun'] == FLORESTA_ID), None)

if not floresta_init:
    print(f"   [ERRO] Floresta não encontrado em initialization.json!")
    sys.exit(1)

# 4. Analisar fluxo principal
print(f"\n[4] Análise do fluxo principal de Floresta:")

modal_matriz = floresta_init.get('modal_matriz', {})
if not modal_matriz:
    print(f"   [AVISO] Sem dados de fluxo!")
else:
    # Agregar fluxos por destino
    flows_by_dest = {}
    total_viagens = 0
    
    for modal, destinos in modal_matriz.items():
        if isinstance(destinos, dict):
            for dest_str, viagens in destinos.items():
                dest_int = int(dest_str)
                flows_by_dest[dest_int] = flows_by_dest.get(dest_int, 0) + viagens
                total_viagens += viagens
    
    print(f"   Total de viagens: {total_viagens:,}")
    print(f"\n   Top 5 destinos:")
    
    top_destinos = sorted(flows_by_dest.items(), key=lambda x: x[1], reverse=True)[:5]
    
    for rank, (dest_cd, viagens) in enumerate(top_destinos, 1):
        # Buscar nome e UTP do destino
        dest_mun = next((m for m in init_data.get('municipios', []) if m['cd_mun'] == dest_cd), None)
        dest_nome = dest_mun['nm_mun'] if dest_mun else f"CD {dest_cd}"
        dest_utp = dest_mun.get('utp_id', 'N/A') if dest_mun else 'N/A'
        dest_rm = dest_mun.get('regiao_metropolitana', 'SEM_RM') if dest_mun else 'N/A'
        
        proporcao = (viagens / total_viagens) * 100
        
        print(f"   {rank}. {dest_nome} (IBGE: {dest_cd})")
        print(f"      - UTP: {dest_utp}")
        print(f"      - RM: {dest_rm}")
        print(f"      - Viagens: {viagens:,} ({proporcao:.1f}%)")
        
        # Se o destino está na UTP 321, destacar
        if dest_utp == EXPECTED_UTP:
            print(f"      *** DESTINO NA UTP ESPERADA (321) ***")

# 5. Verificar no log de border validation
print(f"\n[5] Verificando logs de border validation...")
result_path = Path("data/03_processed/border_validation_result.csv")

if result_path.exists():
    df = pd.read_csv(result_path)
    
    # Buscar Floresta nos logs
    floresta_logs = df[df['mun_id'] == FLORESTA_ID]
    
    if len(floresta_logs) == 0:
        print(f"   ⚠️ Floresta NÃO aparece nos logs de border validation!")
        print(f"   Isso significa que:")
        print(f"      a) Floresta não é um município de fronteira, OU")
        print(f"      b) O fluxo de Floresta não aponta para uma UTP diferente, OU")
        print(f"      c) Floresta é uma Sede (não pode mover)")
    else:
        print(f"   ✓ Floresta aparece {len(floresta_logs)} vez(es) nos logs:")
        print()
        for _, log in floresta_logs.iterrows():
            print(f"   Iteração {log['iteration']}:")
            print(f"      Ação: {log['action']}")
            print(f"      UTP Origem: {log['utp_origem']}")
            print(f"      UTP Destino: {log['utp_destino']}")
            print(f"      Razão: {log['reason']}")
            if pd.notna(log.get('details')):
                print(f"      Detalhes: {log['details']}")
            print()
else:
    print(f"   [AVISO] Arquivo de resultados não encontrado!")

# 6. Verificar adjacência com UTP 321
print(f"\n[6] Verificando adjacência com UTP 321...")

# Carregar GeoDataFrame para análise espacial
try:
    import geopandas as gpd
    
    gdf_path = Path("data/02_interim/municipalities_base.gpkg")
    if gdf_path.exists():
        gdf = gpd.read_file(gdf_path)
        
        # Verificar se Floresta está no GDF
        floresta_geom = gdf[gdf['CD_MUN'] == FLORESTA_ID]
        if len(floresta_geom) > 0:
            print(f"   ✓ Floresta encontrado no GeoDataFrame")
            
            # Verificar vizinhos espaciais
            floresta_row = floresta_geom.iloc[0]
            buf_val = 0.001  # Buffer pequeno para robustez
            neighbors = gdf[gdf.geometry.intersects(floresta_row.geometry.buffer(buf_val))]
            neighbors = neighbors[neighbors['CD_MUN'] != FLORESTA_ID]
            
            print(f"   Vizinhos espaciais de Floresta: {len(neighbors)}")
            
            # Verificar se algum vizinho está na UTP 321
            neighbors_utp_321 = []
            for _, neighbor in neighbors.iterrows():
                neighbor_id = int(neighbor['CD_MUN'])
                neighbor_utp = graph.get_municipality_utp(neighbor_id)
                if neighbor_utp == EXPECTED_UTP:
                    neighbor_name = graph.hierarchy.nodes.get(neighbor_id, {}).get('name', f'Mun_{neighbor_id}')
                    neighbors_utp_321.append(neighbor_name)
            
            if neighbors_utp_321:
                print(f"   ✓ Floresta é adjacente à UTP {EXPECTED_UTP}!")
                print(f"   Vizinhos na UTP 321: {', '.join(neighbors_utp_321)}")
            else:
                print(f"   ✗ Floresta NÃO é adjacente à UTP {EXPECTED_UTP}")
                print(f"   Este é o problema! Floresta não faz fronteira com UTP 321.")
        else:
            print(f"   [ERRO] Floresta não encontrado no GeoDataFrame!")
    else:
        print(f"   [AVISO] GeoDataFrame não encontrado: {gdf_path}")
        
except Exception as e:
    print(f"   [ERRO] Falha ao carregar GeoDataFrame: {e}")

print("\n" + "=" * 80)
print("FIM DO DIAGNÓSTICO")
print("=" * 80)
