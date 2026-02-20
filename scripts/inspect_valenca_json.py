
import json
import os

json_path = 'data/initialization.json'

with open(json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)
    
muns = data.get('municipios', [])
found = False
for m in muns:
    if str(m.get('cd_mun')) == '2932900':
        print(json.dumps(m, indent=2, ensure_ascii=False))
        found = True
        break
        
if not found:
    print("Municipality 2932900 NOT FOUND in initialization.json")
