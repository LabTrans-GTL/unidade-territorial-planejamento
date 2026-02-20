
import sys
import asyncio
from pathlib import Path

# Setup paths
PROJECT_ROOT = Path(r"c:\Users\vinicios.buzzi\buzzi\geovalida")
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "frontend" / "frontend"))

from amcharts_generator import AmChartsMapGenerator

def test_generation():
    print("Testing amCharts generation...")
    data_root = PROJECT_ROOT / "data"
    generator = AmChartsMapGenerator(data_root)
    
    output_path = PROJECT_ROOT / "frontend" / "assets" / "map_test.html"
    
    # Ensure assets dir exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    success = generator.generate_and_save("step8", "Teste Mapa", output_path)
    
    if success:
        print(f"✅ Map generated successfully at {output_path}")
        print(f"File size: {output_path.stat().st_size / 1024:.2f} KB")
        # Check content briefly
        with open(output_path, 'r', encoding='utf-8') as f:
            content = f.read()
            if "am5.Root.new" in content and "geoJSON" in content:
                print("✅ Content looks correct (contains amCharts init)")
            else:
                print("❌ Content verification failed")
    else:
        print("❌ Map generation failed")

if __name__ == "__main__":
    test_generation()
