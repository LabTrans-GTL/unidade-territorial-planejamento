import json

with open('data/sede_analysis_cache.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Get all sede IDs
all_sedes = {int(s['cd_mun_sede']) for s in data['sede_analysis']}

# Check the 3 alerts
alerts = [s for s in data['sede_analysis'] if s.get('tem_alerta_dependencia')]

print(f"Total sedes: {len(all_sedes)}")
print(f"Total alerts: {len(alerts)}\n")

for s in alerts:
    origem_id = int(s['cd_mun_sede'])
    destino_id = int(s.get('principal_destino_cd', 0))
    
    print(f"\n{'='*80}")
    print(f"ORIGEM: {s['nm_sede']} ({origem_id})")
    print(f"  UTP: {s['utp_id']}")
    print(f"  Aeroporto: {s['tem_aeroporto']}")
    print(f"  Turismo: {s['turismo']}")
    print(f"  REGIC: {s['regic']}")
    
    print(f"\nDESTINO: {s['principal_destino_nm']} ({destino_id})")
    print(f"  É Sede?: {destino_id in all_sedes}")
    
    if destino_id in all_sedes:
        # Find destination details
        dest_info = next((d for d in data['sede_analysis'] if d['cd_mun_sede'] == destino_id), None)
        if dest_info:
            print(f"  UTP: {dest_info['utp_id']}")
            print(f"  Aeroporto: {dest_info['tem_aeroporto']}")
            print(f"  Turismo: {dest_info['turismo']}")
            print(f"  REGIC: {dest_info['regic']}")
    else:
        print(f"  ⚠️  DESTINO NÃO É SEDE!")
    
    print(f"\nFLUXO:")
    print(f"  Tempo: {s['tempo_ate_destino_h']:.2f}h")
    print(f"  Proporção: {s['proporcao_fluxo_principal']:.2%}")
    print(f"  Viagens: {int(s['viagens_para_destino']):,}")
