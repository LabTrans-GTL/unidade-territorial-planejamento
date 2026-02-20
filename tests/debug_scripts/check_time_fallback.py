"""
Script to verify travel time using Manager/MapGenerator fallback
"""
from src.core.manager import GeoValidaManager

print("Initializing Manager...")
m = GeoValidaManager()
m.step_0_initialize_data()

# Ensure SedeAnalyzer has the map generator
print("Checking SedeAnalyzer...")
if not m.sede_analyzer.map_generator:
    print("WARNING: SedeAnalyzer has no map_generator!")
    # Manually attach if needed (though manager does it)
    m.sede_analyzer.map_generator = m.map_generator
else:
    print("SedeAnalyzer has map_generator. OK.")

print("Checking Shapefiles in MapGenerator...")
if m.map_generator.gdf_complete is None:
    print("Loading shapefiles...")
    m.map_generator.load_shapefile(m.config.shapefile_path)
    print(f"Loaded {len(m.map_generator.gdf_complete)} geometries.")

# Check time
orig = 2601607 # Belém
dest = 2603009 # Cabrobó

print(f"\nChecking Time: Belém ({orig}) -> Cabrobó ({dest})")
time = m.sede_analyzer.get_travel_time(orig, dest)

if time is None:
    print("RESULT: Time is None (Not in matrix AND Fallback failed)")
else:
    print(f"RESULT: Time = {time:.4f} hours")
    if time <= 2.0:
        print("✅ LESS than 2h -> Should trigger alert!")
    else:
        print("❌ GREATER than 2h -> Alert will NOT trigger.")

# Check distance manually via map_generator just to be sure
dist_km = m.map_generator.get_distance_km(orig, dest)
print(f"\nGeodesic Distance: {dist_km:.2f} km")
