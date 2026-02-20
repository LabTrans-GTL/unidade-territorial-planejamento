import json
from pathlib import Path

# Carregar initialization.json
data_path = Path("data/initialization.json")
with open(data_path, encoding='utf-8') as f:
    data = json.load(f)

municipios = data.get('municipios', [])

# Verificar estado atual das 3 cidades
cidades_interesse = {
    2612208: "Salgueiro",
    2603009: "Cabrobó", 
    2601607: "Belém do São Francisco"
}

print("=" * 80)
print("ESTADO ATUAL DAS UTPs")
print("=" * 80)

for cd_mun, nome in cidades_interesse.items():
    mun = next((m for m in municipios if m['cd_mun'] == cd_mun), None)
    if mun:
        print(f"\n{nome} (CD: {cd_mun})")
        print(f"  UTP Atual: {mun.get('utp_id')}")
        print(f"  E Sede? {mun.get('sede_utp', False)}")

# Verificar quais municípios estão nas UTPs mencionadas
print("\n" + "=" * 80)
print("COMPOSICAO DAS UTPs RELEVANTES")
print("=" * 80)

utps_check = ['675', '366', '409', '321']

for utp_id in utps_check:
    munis_in_utp = [m for m in municipios if str(m.get('utp_id')) == str(utp_id)]
    
    if not munis_in_utp:
        print(f"\nUTP {utp_id}: (VAZIA)")
        continue
    
    sede = next((m for m in munis_in_utp if m.get('sede_utp')), None)
    
    print(f"\nUTP {utp_id}:")
    if sede:
        print(f"  Sede: {sede['nm_mun']} (CD: {sede['cd_mun']})")
    else:
        print(f"  Sede: [NENHUMA - ORFÃ]")
    
    print(f"  Total de municipios: {len(munis_in_utp)}")
    
    # Mostrar nossos 3 municípios se estiverem nesta UTP
    for cd_check, nome_check in cidades_interesse.items():
        if any(m['cd_mun'] == cd_check for m in munis_in_utp):
            print(f"    -> Contem: {nome_check} (CD: {cd_check})")

print("\n" + "=" * 80)
