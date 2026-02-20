import json

with open('data/sede_analysis_cache.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

alerts = [s for s in data['sede_analysis'] if s.get('tem_alerta_dependencia')]

print(f"Total sedes com alertas: {len(alerts)}")
print(f"Total sedes: {data['summary']['total_sedes']}")
print("\n=== Sedes com Alertas ===")
for s in alerts[:20]:
    print(f"\n{s['nm_sede']} ({s['cd_mun_sede']})")
    print(f"  -> Destino: {s['principal_destino_nm']} ({s.get('principal_destino_cd')})")
    print(f"  -> Tempo: {s['tempo_ate_destino_h']:.2f}h")
    print(f"  -> Aeroporto Origem: {s['tem_aeroporto']}")
    print(f"  -> Turismo: {s['turismo']}")
    print(f"  -> Alerta: {s['alerta_detalhes']}")
