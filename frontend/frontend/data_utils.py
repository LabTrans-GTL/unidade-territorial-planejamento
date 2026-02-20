
import sys
import os
from pathlib import Path
import geopandas as gpd
import pandas as pd
import json
import logging
from functools import lru_cache

# --- Setup Paths ---
PROJECT_ROOT = Path(__file__).parents[2]
if str(PROJECT_ROOT) not in sys.path:
    # Add project root to sys.path to allow imports from src
    sys.path.append(str(PROJECT_ROOT))

# Import Backend Utilities
try:
    from src.utils import DataLoader
except ImportError:
    # Fallback if src is not found (e.g. running script directly without path setup)
    logging.warning("Could not import DataLoader from src via standard import.")

# Setup Logger
logger = logging.getLogger(__name__)

# --- Global Data Caching ---

@lru_cache(maxsize=1)
def get_municipios_df_cached():
    try:
        from src.utils import DataLoader # Local import to handle path setup timing
        loader = DataLoader()
        return loader.get_municipios_dataframe()
    except Exception as e:
        logger.error(f"Error loading municipalities DF: {e}")
        return None

@lru_cache(maxsize=1)
def get_municipalities_gdf_cached():
    """Loads and optimizes the municipalities GeoJSON."""
    try:
        path = PROJECT_ROOT / "data" / "04_maps" / "municipalities_optimized.geojson"
        if not path.exists():
            logger.warning(f"Optimized map file not found: {path}")
            return None
        
        gdf = gpd.read_file(path)
        
        # Ensure compatibility with pipeline data
        if 'CD_MUN' in gdf.columns:
            gdf['CD_MUN'] = gdf['CD_MUN'].astype(str)
        if 'cd_mun' in gdf.columns:
            gdf['cd_mun'] = gdf['cd_mun'].astype(str)
            
        return gdf
    except Exception as e:
        logger.error(f"Error loading municipalities GDF: {e}")
        return None

@lru_cache(maxsize=1)
def get_rm_gdf_cached():
    try:
        path = PROJECT_ROOT / "data" / "04_maps" / "rm_boundaries_optimized.geojson"
        if path.exists():
            return gpd.read_file(path)
    except Exception as e:
        logger.error(f"Error loading RM GDF: {e}")
    return None

@lru_cache(maxsize=1)
def get_states_gdf_cached():
    try:
        path = PROJECT_ROOT / "data" / "04_maps" / "state_boundaries_optimized.geojson"
        if path.exists():
            return gpd.read_file(path)
    except Exception as e:
        logger.error(f"Error loading States GDF: {e}")
    return None

@lru_cache(maxsize=1)
def get_coloring_cached():
    try:
        path = PROJECT_ROOT / "data" / "initial_coloring.json"
        if not path.exists():
            path = PROJECT_ROOT / "data" / "03_processed" / "initial_coloring.json"
        
        if path.exists():
            with open(path, 'r') as f:
                data = json.load(f)
                return {int(k): v for k, v in data.items()}
    except Exception as e:
        logger.error(f"Error loading coloring: {e}")
    return {}
