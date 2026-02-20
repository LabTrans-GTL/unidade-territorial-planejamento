import json
from pathlib import Path

# Load initialization.json
data_path = Path("c:/Users/vinicios.buzzi/buzzi/geovalida/data/initialization.json")

print(f"Loading {data_path}...")
try:
    with open(data_path, encoding='utf-8') as f:
        data = json.load(f)
    print("Loaded.")
except Exception as e:
    print(f"Error loading: {e}")
    exit(1)

municipios = data.get('municipios', [])
print(f"Total municipios: {len(municipios)}")

targets = {
    2506905: "Itabaiana",
    2507507: "João Pessoa"
}

for cd, name in targets.items():
    mun = next((m for m in municipios if m['cd_mun'] == cd), None)
    if mun:
        rm = mun.get('regiao_metropolitana', 'N/A')
        # Check explicit None or empty string
        rm_val = f"'{rm}'" if rm is not None else "None"
        print(f"{name} ({cd}): RM = {rm_val}")
    else:
        print(f"{name} ({cd}): Not Found")
