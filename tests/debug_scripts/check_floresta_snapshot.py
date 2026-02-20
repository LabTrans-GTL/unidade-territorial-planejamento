"""
Script para diagnosticar o caso de Floresta (2605707) diretamente do snapshot.
"""
import json
from pathlib import Path
import pandas as pd

FLORESTA_ID = 2605707
EXPECTED_UTP = "321"

print("=" * 80)
print(f"DIAGNÓSTICO: Floresta (IBGE {FLORESTA_ID})")
print("=" * 80)

# 1. Carregar snapshot
snapshot_path = Path("data/03_processed/snapshot_step8_border_validation.json")

if not snapshot_path.exists():
    print(f"\n[ERRO] Snapshot não encontrado: {snapshot_path}")
    print("Tentando snapshot anterior...")
    # Tentar snapshot do step 7
    snapshot_path = Path("data/03_processed/snapshot_step7_sede_consolidation.json")
    if not snapshot_path.exists():
        print("Nenhum snapshot encontrado!")
        exit(1)

print(f"\n[1] Carregando snapshot: {snapshot_path.name}")
with open(snapshot_path, encoding='utf-8') as f:
    snapshot = json.load(f)

# 2. Procurar Floresta nos nós
print(f"\n[2] Procurando Floresta no snapshot...")

floresta_node = None
if 'hierarchy' in snapshot and 'nodes' in snapshot['hierarchy']:
    for node_id, node_data in snapshot['hierarchy']['nodes'].items():
        # Tentar como int e como string
        try:
            if int(node_id) == FLORESTA_ID:
                floresta_node = node_data
                break
        except:
            pass

if not floresta_node:
    print(f"   [X] Floresta ({FLORESTA_ID}) NAO encontrado no snapshot!")
    print(f"\n   Procurando em initialization.json...")
    
    init_path = Path("data/initialization.json")
    if init_path.exists():
        with open(init_path, encoding='utf-8') as f:
            init_data = json.load(f)
        
        floresta_init = next((m for m in init_data.get('municipios', []) if m['cd_mun'] == FLORESTA_ID), None)
        if floresta_init:
            print(f"   [+] Floresta encontrado em initialization.json:")
            print(f"      Nome: {floresta_init.get('nm_mun')}")
            print(f"      UF: {floresta_init.get('uf')}")
            print(f"      UTP: {floresta_init.get('utp_id')}")
            print(f"      E Sede: {floresta_init.get('sede_utp', False)}")
        else:
            print(f"   [X] Floresta tambem nao esta em initialization.json!")
    exit(1)

print(f"   [+] Floresta encontrado no snapshot!")
print(f"\n[3] Dados de Floresta:")
print(f"   Nome: {floresta_node.get('name', 'N/A')}")
print(f"   Tipo: {floresta_node.get('type', 'N/A')}")
print(f"   UF: {floresta_node.get('uf', 'N/A')}")
print(f"   É Sede: {floresta_node.get('sede_utp', False)}")
print(f"   RM: {floresta_node.get('regiao_metropolitana', 'SEM_RM')}")

# Encontrar UTP atual de Floresta através dos edges
current_utp = None
if 'hierarchy' in snapshot and 'edges' in snapshot['hierarchy']:
    for edge in snapshot['hierarchy']['edges']:
        if str(edge.get('target')) == str(FLORESTA_ID):
            current_utp = edge.get('source')
            break

if current_utp:
    print(f"   UTP Atual: {current_utp}")
    print(f"   UTP Esperada: {EXPECTED_UTP}")
    
    if current_utp == EXPECTED_UTP:
        print(f"\n   [+++] FLORESTA JA ESTA NA UTP CORRETA! [+++]")
    else:
        print(f"\n   [X] Floresta NAO esta na UTP esperada")
else:
    print(f"   UTP Atual: NÃO ENCONTRADA")

# 4. Análise dos fluxos de Floresta
print(f"\n[4] Analisando fluxos de Floresta...")

init_path = Path("data/initialization.json")
if init_path.exists():
    with open(init_path, encoding='utf-8') as f:
        init_data = json.load(f)
    
    floresta_init = next((m for m in init_data.get('municipios', []) if m['cd_mun'] == FLORESTA_ID), None)
    
    if floresta_init and 'modal_matriz' in floresta_init:
        modal_matriz = floresta_init['modal_matriz']
        
        # Agregar fluxos
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
        
        # Criar mapa de município -> UTP do snapshot
        mun_to_utp = {}
        if 'hierarchy' in snapshot and 'edges' in snapshot['hierarchy']:
            for edge in snapshot['hierarchy']['edges']:
                try:
                    target_id = int(edge.get('target'))
                    source_utp = edge.get('source')
                    mun_to_utp[target_id] = source_utp
                except:
                    pass
        
        for rank, (dest_cd, viagens) in enumerate(top_destinos, 1):
            # Buscar info do destino
            dest_mun = next((m for m in init_data.get('municipios', []) if m['cd_mun'] == dest_cd), None)
            dest_nome = dest_mun['nm_mun'] if dest_mun else f"CD {dest_cd}"
            
            # UTP do destino no snapshot atual
            dest_utp = mun_to_utp.get(dest_cd, 'N/A')
            dest_rm = dest_mun.get('regiao_metropolitana', 'SEM_RM') if dest_mun else 'N/A'
            
            proporcao = (viagens / total_viagens) * 100
            
            print(f"\n   {rank}. {dest_nome} (IBGE: {dest_cd})")
            print(f"      - UTP: {dest_utp}")
            print(f"      - RM: {dest_rm}")
            print(f"      - Viagens: {viagens:,} ({proporcao:.1f}%)")
            
            if dest_utp == EXPECTED_UTP:
                print(f"      *** ESTE DESTINO ESTÁ NA UTP {EXPECTED_UTP}! ***")

# 5. Verificar logs de border validation
print(f"\n[5] Verificando logs de border validation...")
result_path = Path("data/03_processed/border_validation_result.csv")

if result_path.exists():
    df = pd.read_csv(result_path)
    
    floresta_logs = df[df['mun_id'] == FLORESTA_ID]
    
    if len(floresta_logs) == 0:
        print(f"   [X] Floresta NAO aparece nos logs de border validation!")
        print(f"\n   Possiveis razoes:")
        print(f"      a) Floresta nao e um municipio de fronteira (border)")
        print(f"      b) O fluxo de Floresta nao aponta para uma UTP diferente")
        print(f"      c) Floresta e uma Sede (ancora, nao pode mover)")
        print(f"      d) A UTP de destino nao e adjacente a UTP atual de Floresta")
    else:
        print(f"   [+] Floresta aparece {len(floresta_logs)} vez(es) nos logs:")
        for _, log in floresta_logs.iterrows():
            print(f"\n   Iteração {log['iteration']}:")
            print(f"      Ação: {log['action']}")
            print(f"      UTP Origem: {log['utp_origem']}")
            print(f"      UTP Destino: {log['utp_destino']}")
            print(f"      Razão: {log['reason']}")
            if pd.notna(log.get('details')):
                print(f"      Detalhes: {log['details']}")
else:
    print(f"   [AVISO] Logs não encontrados!")

# 6. Verificar se Floresta é município de fronteira
print(f"\n[6] Verificando status de fronteira...")

# Listar municípios vizinhos no snapshot
if 'hierarchy' in snapshot and 'nodes' in snapshot['hierarchy']:
    # Precisamos de geometrias para isso - vamos verificar no GDF se disponível
    try:
        import geopandas as gpd
        
        gdf_path = Path("data/02_interim/municipalities_base.gpkg")
        if gdf_path.exists():
            gdf = gpd.read_file(gdf_path)
            
            floresta_geom = gdf[gdf['CD_MUN'] == FLORESTA_ID]
            if len(floresta_geom) > 0:
                print(f"   [+] Analisando vizinhos espaciais...")
                
                floresta_row = floresta_geom.iloc[0]
                buf_val = 0.001
                neighbors = gdf[gdf.geometry.intersects(floresta_row.geometry.buffer(buf_val))]
                neighbors = neighbors[neighbors['CD_MUN'] != FLORESTA_ID]
                
                print(f"   Numero de vizinhos: {len(neighbors)}")
                
                # Verificar UTPs dos vizinhos
                utps_vizinhas = set()
                for _, neighbor in neighbors.iterrows():
                    neighbor_id = int(neighbor['CD_MUN'])
                    neighbor_utp = mun_to_utp.get(neighbor_id, 'N/A')
                    utps_vizinhas.add(neighbor_utp)
                
                print(f"   UTPs vizinhas: {sorted(utps_vizinhas)}")
                
                if current_utp in utps_vizinhas:
                    utps_vizinhas.remove(current_utp)
                
                if utps_vizinhas:
                    print(f"   [+] Floresta E um municipio de fronteira!")
                    print(f"   UTPs adjacentes diferentes: {sorted(utps_vizinhas)}")
                    
                    if EXPECTED_UTP in utps_vizinhas:
                        print(f"\n   [++] UTP {EXPECTED_UTP} E ADJACENTE a Floresta!")
                    else:
                        print(f"\n   [XX] UTP {EXPECTED_UTP} NAO e adjacente a Floresta!")
                        print(f"   ESTE E O PROBLEMA: Floresta nao faz fronteira com UTP {EXPECTED_UTP}")
                else:
                    print(f"   [X] Floresta NAO e municipio de fronteira (sem vizinhos de outras UTPs)")
    except Exception as e:
        print(f"   [ERRO] Não foi possível analisar geometrias: {e}")

print("\n" + "=" * 80)
print("FIM DO DIAGNÓSTICO")
print("=" * 80)
