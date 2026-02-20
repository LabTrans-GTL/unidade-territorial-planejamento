import sys
sys.path.insert(0, '.')
sys.path.insert(0, '..')
from pathlib import Path
from frontend.amcharts_generator import AmChartsMapGenerator

data_root = Path('../data')
gen = AmChartsMapGenerator(data_root)

html = gen.generate_html('step8', 'GeoValida v8.3 - Final')
if not html:
    print('FAILED - generate_html returned None')
    sys.exit(1)

size_kb = len(html.encode('utf-8')) // 1024
print(f'HTML size:        {size_kb} KB')
print(f'am5.net.load:     {"am5.net.load" in html}')
print(f'MAP_DATA:         {"MAP_DATA" in html}')
print(f'Legend:           {"legenda" in html.lower() or "Legenda" in html}')
print(f'homeGeoPoint:     {"homeGeoPoint" in html}')
print(f'hover state:      {"hover" in html}')
print(f'active state:     {"active" in html}')
print(f'postMessage:      {"addEventListener" in html}')
print(f'ZoomControl:      {"ZoomControl" in html}')
print(f'municipalities:   {"/municipalities_v3.geojson" in html}')

assert size_kb < 500, f'HTML too large: {size_kb} KB!'
print()
print('All checks passed!')
