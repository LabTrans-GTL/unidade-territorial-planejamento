import json
from pathlib import Path

# Carregar initialization.json
data_path = Path("data/initialization.json")
with open(data_path, encoding='utf-8') as f:
    data = json.load(f)

municipios = data.get('municipios', [])

# Cidades para verificar
cidades = {
    2905404: "Cairu",
    2932903: "Valença",
    2931202: "Taperoá",
}


print("=" * 80)
print("ANÁLISE DE FLUXOS PRINCIPAIS")
print("=" * 80)

for cd_mun, nome in cidades.items():
    # Encontrar município
    mun = next((m for m in municipios if m['cd_mun'] == cd_mun), None)
    
    if not mun:
        print(f"\n[ERRO] {nome} ({cd_mun}) NAO ENCONTRADO")
        continue
    
    print(f"\n{'=' * 80}")
    print(f"[MUNICIPIO] {mun['nm_mun']} ({cd_mun})")
    print(f"   UTP: {mun.get('utp_id', 'N/A')}")
    print(f"   UF: {mun.get('uf', 'N/A')}")
    print(f"   E Sede? {mun.get('sede_utp', False)}")
    print(f"{'=' * 80}")
    
    modal_matriz = mun.get('modal_matriz', {})
    
    if not modal_matriz:
        print("   [AVISO] SEM DADOS DE FLUXO")
        continue
    
    # Agregar fluxos por destino (soma todos os modais)
    flows_by_dest = {}
    total_viagens = 0
    
    for modal, destinos in modal_matriz.items():
        if isinstance(destinos, dict):
            for dest_str, viagens in destinos.items():
                dest_int = int(dest_str)
                flows_by_dest[dest_int] = flows_by_dest.get(dest_int, 0) + viagens
                total_viagens += viagens
    
    if not flows_by_dest:
        print("   [AVISO] SEM FLUXOS REGISTRADOS")
        continue
    
    print(f"\n   [TOTAL] TOTAL DE VIAGENS: {total_viagens:,}")
    print(f"\n   [TOP 5] TOP 5 DESTINOS:")
    print(f"   {'-' * 76}")
    
    # Ordenar por número de viagens
    top_destinos = sorted(flows_by_dest.items(), key=lambda x: x[1], reverse=True)[:5]
    
    for rank, (dest_cd, viagens) in enumerate(top_destinos, 1):
        # Buscar nome do destino
        dest_mun = next((m for m in municipios if m['cd_mun'] == dest_cd), None)
        dest_nome = dest_mun['nm_mun'] if dest_mun else f"CD {dest_cd}"
        dest_utp = dest_mun.get('utp_id', 'N/A') if dest_mun else 'N/A'
        dest_sede = "[SEDE]" if dest_mun and dest_mun.get('sede_utp') else ""
        
        proporcao = (viagens / total_viagens) * 100
        
        print(f"   {rank}. {dest_nome} (CD: {dest_cd})")
        print(f"      +-- UTP: {dest_utp} {dest_sede}")
        print(f"      +-- Viagens: {viagens:,} ({proporcao:.1f}%)")
        print()
    
    # Mostrar distribuição por modal
    print(f"\n   [MODAIS] DISTRIBUICAO POR MODAL:")
    print(f"   {'-' * 76}")
    
    modal_totals = {}
    for modal, destinos in modal_matriz.items():
        if isinstance(destinos, dict):
            total = sum(destinos.values())
            if total > 0:
                modal_totals[modal] = total
    
    modal_names = {
        'rodoviaria_coletiva': 'Rodoviária Coletiva',
        'rodoviaria_particular': 'Rodoviária Particular',
        'aeroviaria': 'Aeroviária',
        'ferroviaria': 'Ferroviária',
        'hidroviaria': 'Hidroviária'
    }
    
    for modal, total in sorted(modal_totals.items(), key=lambda x: x[1], reverse=True):
        modal_nome = modal_names.get(modal, modal)
        proporcao = (total / total_viagens) * 100
        print(f"   - {modal_nome}: {total:,} ({proporcao:.1f}%)")

print("\n" + "=" * 80)
print("FIM DA ANÁLISE")
print("=" * 80)
