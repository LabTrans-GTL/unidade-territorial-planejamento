"""
Diagnostico direto: Por que Floresta nao esta na UTP de Serra Talhada (321)?
"""
import json
import pandas as pd
from pathlib import Path

FLORESTA_ID = 2605707
SERRA_TALHADA_ID = 2613909  # Codigo IBGE de Serra Talhada
EXPECTED_UTP = "321"

print("=" * 80)
print("DIAGNOSTICO: Por que Floresta nao esta na UTP 321 (Serra Talhada)?")
print("=" * 80)

# 1. Verificar UTP de Serra Talhada
print("\n[1] Verificando UTP de Serra Talhada...")
init_path = Path("data/initialization.json")
with open(init_path, encoding='utf-8') as f:
    init_data = json.load(f)

serra_talhada = next((m for m in init_data['municipios'] if m['cd_mun'] == SERRA_TALHADA_ID), None)
if serra_talhada:
    print(f"   Serra Talhada: UTP = {serra_talhada.get('utp_id')}, Sede = {serra_talhada.get('sede_utp')}")
    serra_utp = serra_talhada.get('utp_id')
else:
    print(f"   [ERRO] Serra Talhada nao encontrada!")
    serra_utp = EXPECTED_UTP

# 2. Verificar situacao de Floresta
print(f"\n[2] Situacao de Floresta...")
floresta = next((m for m in init_data['municipios'] if m['cd_mun'] == FLORESTA_ID), None)
if floresta:
    print(f"   Floresta:")
    print(f"      UTP inicial: {floresta.get('utp_id')}")
    print(f"      UF: {floresta.get('uf')}")
    print(f"      E Sede: {floresta.get('sede_utp', False)}")
    print(f"      RM: {floresta.get('regiao_metropolitana', 'SEM_RM')}")
    floresta_utp_inicial = floresta.get('utp_id')
else:
    print(f"   [ERRO] Floresta nao encontrada!")
    exit(1)

# 3. Verificar nos snapshots
print(f"\n[3] Verificando snapshots...")

# Snapshot Step 7 (Consolidacao de Sedes)
step7_path = Path("data/03_processed/snapshot_step7_sede_consolidation.json")
if step7_path.exists():
    with open(step7_path, encoding='utf-8') as f:
        step7 = json.load(f)
    
    # Encontrar UTP de Floresta no step 7
    floresta_utp_step7 = None
    if 'hierarchy' in step7 and 'edges' in step7['hierarchy']:
        for edge in step7['hierarchy']['edges']:
            if str(edge.get('target')) == str(FLORESTA_ID):
                floresta_utp_step7 = edge.get('source')
                break
    
    print(f"   Step 7 (Consolidacao Sedes): Floresta em UTP {floresta_utp_step7}")
else:
    print(f"   [AVISO] Snapshot step 7 nao encontrado!")
    floresta_utp_step7 = floresta_utp_inicial

# Snapshot Step 8 (Border Validation)
step8_path = Path("data/03_processed/snapshot_step8_border_validation.json")
if step8_path.exists():
    with open(step8_path, encoding='utf-8') as f:
        step8 = json.load(f)
    
    # Encontrar UTP de Floresta no step 8
    floresta_utp_step8 = None
    if 'hierarchy' in step8 and 'edges' in step8['hierarchy']:
        for edge in step8['hierarchy']['edges']:
            if str(edge.get('target')) == str(FLORESTA_ID):
                floresta_utp_step8 = edge.get('source')
                break
    
    print(f"   Step 8 (Border Validation): Floresta em UTP {floresta_utp_step8}")
else:
    print(f"   [AVISO] Snapshot step 8 nao encontrado!")
    floresta_utp_step8 = floresta_utp_step7

# 4. Resumo da trajetoria
print(f"\n[4] Trajetoria de Floresta:")
print(f"   Inicial: UTP {floresta_utp_inicial}")
print(f"   Apos Step 7: UTP {floresta_utp_step7}")
print(f"   Apos Step 8: UTP {floresta_utp_step8}")
print(f"   Esperado: UTP {serra_utp}")

if floresta_utp_step8 == serra_utp:
    print(f"\n   [OK] Floresta JA esta na UTP de Serra Talhada!")
    exit(0)
else:
    print(f"\n   [PROBLEMA] Floresta NAO esta na UTP de Serra Talhada!")

# 5. Verificar logs de border validation
print(f"\n[5] Verificando logs de border validation...")
result_path = Path("data/03_processed/border_validation_result.csv")

if not result_path.exists():
    print(f"   [ERRO] Logs nao encontrados!")
    exit(1)

df = pd.read_csv(result_path)
floresta_logs = df[df['mun_id'] == FLORESTA_ID]

if len(floresta_logs) == 0:
    print(f"   [CRITICO] Floresta NAO aparece nos logs!")
    print(f"\n   Isso significa que o border validator NAO considerou Floresta.")
    print(f"   Possiveis razoes:")
    print(f"      a) Floresta nao e municipio de fronteira")
    print(f"      b) O fluxo principal de Floresta nao aponta para UTP {serra_utp}")
    print(f"      c) Floresta e sede (nao pode mover)")
else:
    print(f"   [+] Floresta aparece {len(floresta_logs)} vez(es) nos logs:")
    for idx, log in floresta_logs.iterrows():
        print(f"\n   Iteracao {log['iteration']}:")
        print(f"      Acao: {log['action']}")
        print(f"      UTP Origem: {log['utp_origem']}")
        print(f"      UTP Destino: {log['utp_destino']}")
        print(f"      Razao: {log['reason']}")
        if pd.notna(log.get('details')):
            print(f"      Detalhes: {log['details']}")

# 6. Analisar fluxo de Floresta
print(f"\n[6] Analisando fluxo principal de Floresta...")

if 'modal_matriz' in floresta:
    modal_matriz = floresta['modal_matriz']
    
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
    
    # Top 5 destinos
    top_destinos = sorted(flows_by_dest.items(), key=lambda x: x[1], reverse=True)[:5]
    
    print(f"\n   Top 5 destinos:")
    for rank, (dest_cd, viagens) in enumerate(top_destinos, 1):
        dest_mun = next((m for m in init_data['municipios'] if m['cd_mun'] == dest_cd), None)
        dest_nome = dest_mun['nm_mun'] if dest_mun else f"CD {dest_cd}"
        dest_utp = dest_mun.get('utp_id', 'N/A') if dest_mun else 'N/A'
        
        proporcao = (viagens / total_viagens) * 100
        
        print(f"   {rank}. {dest_nome} (IBGE: {dest_cd})")
        print(f"      - UTP: {dest_utp}")
        print(f"      - Viagens: {viagens:,} ({proporcao:.1f}%)")
        
        if dest_cd == SERRA_TALHADA_ID:
            print(f"      *** ESTE E SERRA TALHADA (UTP {serra_utp})! ***")

print("\n" + "=" * 80)
print("CONCLUSAO:")

# Verificar se Serra Talhada esta nos top destinos
serra_flow = flows_by_dest.get(SERRA_TALHADA_ID, 0)
if serra_flow > 0:
    proporcao = (serra_flow / total_viagens) * 100
    print(f"   Floresta TEM fluxo para Serra Talhada: {serra_flow:,} viagens ({proporcao:.1f}%)")
    
    # Verificar se atinge threshold de 3%
    if proporcao >= 3.0:
        print(f"   O fluxo SUPERA o threshold de 3%!")
        print(f"   O problema deve ser:")
        print(f"      - Violacao de regra RM, OU")
        print(f"      - Falta de adjacencia entre UTPs, OU")
        print(f"      - Fragmentacao da UTP origem")
    else:
        print(f"   O fluxo NAO atinge o threshold de 3% (muito fraco)")
else:
    print(f"   Floresta NAO tem fluxo para Serra Talhada!")
    print(f"   O principal destino e outro municipio.")

print("=" * 80)
