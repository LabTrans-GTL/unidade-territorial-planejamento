
import folium
import os
from pathlib import Path

ASSETS_DIR = Path(__file__).parent / "assets"
MAPS_DIR = ASSETS_DIR
MAPS_DIR.mkdir(parents=True, exist_ok=True)

def generate_small_map():
    print("Generating small test map...")
    m = folium.Map(location=[-14.235, -51.925], zoom_start=4)
    folium.Marker(
        [-14.235, -51.925], 
        popup="Small Map Test", 
        tooltip="If you see this, Folium works!"
    ).add_to(m)
    
    file_path = MAPS_DIR / "map_small.html"
    m.save(str(file_path))
    print(f"Saved to {file_path}")

if __name__ == "__main__":
    generate_small_map()
