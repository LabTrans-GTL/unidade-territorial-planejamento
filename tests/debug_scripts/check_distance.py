import json
from math import radians, cos, sin, asin, sqrt

data = json.load(open('data/initialization.json', encoding='utf-8'))
munas = {m['cd_mun']: m for m in data['municipios']}

# Belém do São Francisco -> Cabrobó
b = munas[2601607]
c = munas[2603009]

print(f"Belém: {b['latitude']}, {b['longitude']}")
print(f"Cabrobó: {c['latitude']}, {c['longitude']}")

def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 
    return c * r

dist = haversine(b['longitude'], b['latitude'], c['longitude'], c['latitude'])
print(f"Distancia: {dist:.2f} km")

# Estimativa 1: linha reta / 60km/h (muito otimista)
tempo_reto = dist / 60
print(f"Tempo (Linha Reta 60km/h): {tempo_reto:.2f} h")

# Estimativa 2: Fator de sinuosidade 1.3
tempo_real = (dist * 1.3) / 60
print(f"Tempo (Estimado Rodovia 1.3x): {tempo_real:.2f} h")

# Cabrobó -> Salgueiro
s = munas[2612208]
dist2 = haversine(c['longitude'], c['latitude'], s['longitude'], s['latitude'])
print(f"\nCabrobó -> Salgueiro Distancia: {dist2:.2f} km")
print(f"Tempo (Estimado Rodovia 1.3x): {(dist2 * 1.3) / 60:.2f} h")
