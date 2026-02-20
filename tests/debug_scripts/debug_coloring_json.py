
import json
from pathlib import Path
from collections import Counter

data_dir = Path("data")
initial_path = data_dir / "initial_coloring.json"
consolidated_path = data_dir / "consolidated_coloring.json"

def inspect_coloring(path, name):
    if not path.exists():
        print(f"{name}: NOT FOUND")
        return

    try:
        with open(path, 'r') as f:
            data = json.load(f)
        
        print(f"--- {name} ---")
        print(f"Total municipalities: {len(data)}")
        
        # Analyze distribution of colors
        colors = list(data.values())
        counts = Counter(colors)
        print(f"Color distribution: {dict(counts)}")
        print(f"Number of unique colors: {len(counts)}")
        
        # Check first few items
        print(f"Sample items: {list(data.items())[:5]}")
        
    except Exception as e:
        print(f"{name}: ERROR - {e}")

inspect_coloring(initial_path, "INITIAL")
inspect_coloring(consolidated_path, "CONSOLIDATED")
