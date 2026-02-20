
import geopandas as gpd
import shapely.geometry as sgeom
import pandas as pd
import networkx as nx
import logging
import sys
from pathlib import Path

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.core.graph import TerritorialGraph

def test_coloring_with_gap():
    logging.basicConfig(level=logging.INFO)
    graph = TerritorialGraph()
    graph.logger = logging.getLogger("Test")
    
    # Criar 2 quadrados com um gap de 0.00045 graus (~50m)
    # O buffer de 100m (0.0009 graus) deve ser capaz de "pulá-los"
    # Poly 1: 0,0 a 1,1
    # Poly 2: 1.00045, 0 a 2.00045, 1
    poly1 = sgeom.box(0, 0, 1, 1)
    poly2 = sgeom.box(1.00045, 0, 2.00045, 1)
    
    df = pd.DataFrame({
        'UTP_ID': ['A', 'B'],
        'CD_MUN': [1, 2],
        'geometry': [poly1, poly2]
    })
    gdf = gpd.GeoDataFrame(df, crs="EPSG:4326")
    
    print("\nExecutando coloração com gap de 0.00045 (~50m)...")
    coloring = graph.compute_graph_coloring(gdf)
    
    color_a = coloring.get(1)
    color_b = coloring.get(2)
    
    print(f"Cor UTP A (Mun 1): {color_a}")
    print(f"Cor UTP B (Mun 2): {color_b}")
    
    if color_a != color_b:
        print("SUCCESS: UTPs vizinhas (com gap de 50m) receberam cores diferentes.")
    else:
        print("FAILURE: UTPs vizinhas receberam a mesma cor.")
        sys.exit(1)

if __name__ == "__main__":
    test_coloring_with_gap()
