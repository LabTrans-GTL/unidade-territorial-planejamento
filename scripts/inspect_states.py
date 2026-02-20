
import geopandas as gpd
from pathlib import Path

# Path to the optimized state boundaries file
file_path = Path("data/04_maps/state_boundaries_optimized.geojson")

if not file_path.exists():
    print(f"File not found: {file_path}")
else:
    gdf = gpd.read_file(file_path)
    print(f"Loaded {len(gdf)} features from {file_path}")
    print("Columns:", gdf.columns)
    print("Head:")
    print(gdf[['uf', 'geometry']].head())
    
    # Check if geometries are MultiPolygons with many components
    for idx, row in gdf.iterrows():
        geom = row.geometry
        geom_type = geom.geom_type
        if geom_type == 'MultiPolygon':
            print(f"Row {idx} (UF={row.get('uf')}): MultiPolygon with {len(geom.geoms)} parts")
        else:
            print(f"Row {idx} (UF={row.get('uf')}): {geom_type}")
