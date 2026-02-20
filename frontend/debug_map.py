
import sys
import os
from pathlib import Path
import time
import logging
import asyncio

# Setup Paths
current_dir = Path.cwd()
sys.path.append(str(current_dir))
sys.path.append(str(current_dir.parent))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from frontend.data_utils import (
    get_municipios_df_cached,
    get_municipalities_gdf_cached,
    get_rm_gdf_cached,
    get_states_gdf_cached,
    get_coloring_cached,
    PROJECT_ROOT
)
from src.interface.palette import get_palette
from src.interface.map_flow_render import render_map_with_flow_popups

ASSETS_DIR = Path(__file__).parent.parent / "assets"
MAPS_DIR = ASSETS_DIR
MAPS_DIR.mkdir(parents=True, exist_ok=True)

async def debug_map_gen(version: str):
    print(f"--- Debugging Map Generation v{version} ---")
    
    safe_version = "".join(c for c in version if c.isalnum() or c in ".-_")
    filename = f"map_{safe_version}.html"
    file_path = MAPS_DIR / filename
    
    # Clean prev
    if file_path.exists():
        os.remove(file_path)

    def _generate_sync():
        print("Loading Data...")
        df_muni = get_municipios_df_cached()
        gdf = get_municipalities_gdf_cached()
        
        if df_muni is None or gdf is None:
            print("ERROR: Data None")
            return None

        print(f"Data Loaded. GDF Shape: {gdf.shape}")
        
        gdf_filtered = gdf.copy()
        
        print("Rendering Map...")
        start_render = time.time()
        m = render_map_with_flow_popups(
            gdf_filtered=gdf_filtered,
            df_municipios=df_muni,
            title=f"Versão {version}",
            global_colors=get_coloring_cached(),
            gdf_rm=get_rm_gdf_cached(),
            show_rm_borders=True,
            show_state_borders=True,
            gdf_states=get_states_gdf_cached(),
            PASTEL_PALETTE=get_palette()
        )
        print(f"Render finished in {time.time() - start_render:.2f}s")
        
        return m

    m = await asyncio.to_thread(_generate_sync)
    
    if m is None:
        print("ERROR: Map object is None")
        return

    print(f"Map Object Type: {type(m)}")
    
    # Check content before saving
    print("Checking HTML representation...")
    try:
        html_content = m.get_root().render()
        print(f"Generated HTML Content Length: {len(html_content)}")
        
        if len(html_content) == 0:
            print("CRITICAL: Map HTML is empty!")
            return
            
        # Manually save
        print(f"Saving to {file_path}...")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(html_content)
            
        print("Save completed.")
        
    except Exception as e:
        print(f"ERROR during verification: {e}")

    # Check file
    if file_path.exists():
        size = os.path.getsize(file_path)
        print(f"File Size on Disk: {size} bytes")
    else:
        print("ERROR: File not found on disk after save.")

if __name__ == "__main__":
    asyncio.run(debug_map_gen("8.0"))
