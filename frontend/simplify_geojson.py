
import geopandas as gpd
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def simplify_geojson():
    input_path = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\data\04_maps\municipalities_optimized.geojson")
    output_path = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida\frontend\assets\municipalities.geojson")
    
    logger.info(f"Loading {input_path}...")
    gdf = gpd.read_file(input_path)
    
    logger.info(f"Original CRS: {gdf.crs}")
    # Keep only essential columns to reduce size
    # Check for column names case variants
    cols_to_keep = []
    
    # Standardize column names if needed
    for col in ["CD_MUN", "cd_mun"]:
        if col in gdf.columns:
            cols_to_keep.append(col)
            break
            
    for col in ["NM_MUN", "nm_mun", "name"]:
        if col in gdf.columns:
            cols_to_keep.append(col)
            break
            
    # Always keep geometry
    cols_to_keep.append("geometry")
    
    logger.info(f"Keeping columns: {cols_to_keep}")
    gdf_simplified = gdf[cols_to_keep].copy()
    
    # Simplify
    # Tolerance is in degrees. 0.01 ~ 1.1km. Aggressive but needed for web.
    tolerance = 0.01
    logger.info(f"Simplifying with tolerance {tolerance}...")
    
    gdf_simplified.geometry = gdf.geometry.simplify(tolerance, preserve_topology=True)
    
    # Save
    logger.info(f"Saving to {output_path}...")
    
    # Ensure assets dir exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert to JSON to manipulate feature IDs explicitly
    import json
    geojson_dict = json.loads(gdf_simplified.to_json())
    
    # Promote CD_MUN to top-level 'id' for amCharts data binding
    count = 0
    for feature in geojson_dict.get("features", []):
        props = feature.get("properties", {})
        # Find the ID key
        for key in ["CD_MUN", "cd_mun"]:
            if key in props:
                feature["id"] = str(props[key])
                count += 1
                break
    
    logger.info(f"Assigned IDs to {count} features")
    
    # Save as GeoJSON
    # Using v3 for this iterations
    output_path_v3 = output_path.parent / "municipalities_v3.geojson"
    logger.info(f"Saving to {output_path_v3}...")
    
    with open(output_path_v3, "w", encoding="utf-8") as f:
        json.dump(geojson_dict, f, separators=(',', ':')) # Minified
    
    # Check size
    new_size = output_path_v3.stat().st_size / (1024*1024)
    
    logger.info(f"Done! Saved v3 with IDs. Size: {new_size:.2f} MB")

if __name__ == "__main__":
    simplify_geojson()
