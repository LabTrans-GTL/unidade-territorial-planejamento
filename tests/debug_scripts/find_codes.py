import json

data = json.load(open('data/initialization.json', encoding='utf-8'))
munis = data['municipios']

# Buscar municípios de SC
sc_munis = [m for m in munis if m['uf'] == 'SC']

print("Busca por 'Jaragua':")
for m in sc_munis:
    if 'jaragua' in m['nm_mun'].lower():
        print(f"  {m['nm_mun']}: {m['cd_mun']}")

print("\nBusca por 'Joinville':")
for m in sc_munis:
    if 'joinville' in m['nm_mun'].lower():
        print(f"  {m['nm_mun']}: {m['cd_mun']}")

print("\nBusca por 'Sao Bento':")
for m in sc_munis:
    if 'bento' in m['nm_mun'].lower() and 'sul' in m['nm_mun'].lower():
        print(f"  {m['nm_mun']}: {m['cd_mun']}")
