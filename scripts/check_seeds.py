
import json
from pathlib import Path

data_dir = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\03_processed")
step6_path = data_dir / "snapshot_step6_sede_consolidation.json"

def check_seeds(path):
    print(f"Loading {path}...")
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        utp_seeds = data.get('utp_seeds', {})
        print(f"Total seeds: {len(utp_seeds)}")
        
        seed_293 = utp_seeds.get("293")
        print(f"Seed for UTP 293: {seed_293}")
        
        # Also check if 4316808 shows up as a value anywhere
        found_as_value = False
        for k, v in utp_seeds.items():
            if str(v) == "4316808":
                print(f"4316808 is seed for UTP {k}")
                found_as_value = True
        
        if not found_as_value:
            print("4316808 is NOT a seed for any UTP.")

    except Exception as e:
        print(f"Error: {e}")

check_seeds(step6_path)
