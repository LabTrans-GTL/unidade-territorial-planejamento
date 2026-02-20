
import json

path = r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\sede_analysis_cache.json"

try:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    analysis = data.get('sede_analysis', [])
    print(f"Total entries: {len(analysis)}")
    
    hits = 0
    for entry in analysis:
        if entry.get('tem_alerta_dependencia') == True:
            hits += 1
            print(f"\n--- ALERT FOUND: {entry['nm_sede']} ({entry['cd_mun_sede']}) ---")
            print(f"UTP: {entry['utp_id']}")
            print(f"Target: {entry.get('principal_destino_nm')} ({entry.get('principal_destino_cd')})")
            print(f"Regic: {entry.get('regic')}")
            print(f"Tem Aeroporto: {entry.get('tem_aeroporto')}")
            print(f"Turismo: {entry.get('turismo')}")
            print(json.dumps(entry, indent=2, ensure_ascii=False))

    print(f"\nTotal alerts: {hits}")

except Exception as e:
    print(f"Error: {e}")
