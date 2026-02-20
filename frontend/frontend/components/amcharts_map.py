"""
amCharts Map Component
Custom Reflex component wrapping amCharts 5 for interactive Brazil UTP maps.
"""
import reflex as rx
from pathlib import Path

class AmChartsMapConfig(rx.Base):
    """Configuration for amCharts map"""
    geojson_path: str
    snapshot_path: str  
    center_lat: float = -15.0
    center_lon: float = -47.0
    zoom: int = 4


class AmChartsMap(rx.Component):
    """
    Custom component for rendering Brazil UTP maps with amCharts 5.
    
    This component dynamically loads GeoJSON and snapshot data to visualize
    UTP configurations across different pipeline steps.
    """
    
    library = "@amcharts/amcharts5@5"
    tag = "div"
    
    # Component props
    geojson_url: rx.Var[str]
    snapshot_url: rx.Var[str]
    height: rx.Var[str] = "100%"
    width: rx.Var[str] = "100%"
    
    def _get_custom_code(self) -> str:
        """JavaScript code for amCharts initialization"""
        return """
// amCharts 5 Initialization
import * as am5 from "@amcharts/amcharts5";
import * as am5map from "@amcharts/amcharts5/map";
import am5themes_Animated from "@amcharts/amcharts5/themes/Animated";

// Create root
const root = am5.Root.new(this);

// Set themes
root.setThemes([
  am5themes_Animated.new(root)
]);

// Create chart
const chart = root.container.children.push(
  am5map.MapChart.new(root, {
    panX: "translateX",
    panY: "translateY",
    projection: am5map.geoMercator(),
    homeZoomLevel: 4,
    homeGeoPoint: { longitude: -47, latitude: -15 }
  })
);

// Create polygon series
const polygonSeries = chart.series.push(
  am5map.MapPolygonSeries.new(root, {
    geoJSON: null, // Will be loaded dynamically
    valueField: "value",
    calculateAggregates: true
  })
);

// Load data and render
async function loadMapData() {
  try {
    // Load GeoJSON and snapshot
    const [geojsonData, snapshotData] = await Promise.all([
      fetch(this.props.geojson_url).then(r => r.json()),
      fetch(this.props.snapshot_url).then(r => r.json())
    ]);
    
    // Merge snapshot data into GeoJSON properties
    const coloring = snapshotData.coloring || {};
    const nodes = snapshotData.nodes || {};
    
    geojsonData.features.forEach(feature => {
      const codibge = feature.properties.CD_MUN || feature.properties.cd_mun;
      const nodeData = nodes[codibge] || {};
      
      feature.properties = {
        ...feature.properties,
        utp_id: nodeData.utp_id || 'SEM_UTP',
        sede_utp: nodeData.sede_utp || false,
        regiao_metropolitana: nodeData.regiao_metropolitana || 'SEM_RM',
        nm_mun: nodeData.name || feature.properties.NM_MUN,
        color_id: coloring[codibge] || 0
      };
    });
    
    // Set GeoJSON data
    polygonSeries.set("geoJSON", geojsonData);
    
  } catch (error) {
    console.error("Error loading map data:", error);
  }
}

loadMapData();

// Configure polygon appearance
polygonSeries.mapPolygons.template.setAll({
  tooltipText: "{nm_mun}\\nUTP: {utp_id}\\nSede: {sede_utp}",
  interactive: true,
  fill: am5.color(0xCCCCCC),
  strokeWidth: 0.5,
  stroke: am5.color(0x000000)
});

// Color based on color_id from snapshot
const colorMap = {
  1: 0x2D6A4F,  // Verde escuro
  2: 0x009E2D,  // Verde médio
  3: 0x00CF00,  // Verde claro
  4: 0xFFCF00,  // Amarelo
  5: 0xFF8C00,  // Laranja
  6: 0xFF0000,  // Vermelho
  7: 0x6A0DAD   // Roxo
};

polygonSeries.mapPolygons.template.adapters.add("fill", (fill, target) => {
  const colorId = target.dataItem?.dataContext?.properties?.color_id;
  return colorId ? am5.color(colorMap[colorId] || 0xCCCCCC) : fill;
});

// Add zoom controls
const zoomControl = chart.set("zoomControl", am5map.ZoomControl.new(root, {}));
zoomControl.homeButton.set("visible", true);

// Cleanup on unmount
return () => {
  root.dispose();
};
"""

def get_event_triggers(self) -> dict:
    return {}


def amcharts_map(
    geojson_url: str,
    snapshot_url: str,
    height: str = "100%",
    width: str = "100%",
    **props
) -> rx.Component:
    """
    Factory function to create AmChartsMap component.
    
    Args:
        geojson_url: URL to GeoJSON file with municipality geometries
        snapshot_url: URL to snapshot JSON with UTP assignments
        height: CSS height (default: 100%)
        width: CSS width (default: 100%)
    """
    return AmChartsMap.create(
        geojson_url=geojson_url,
        snapshot_url=snapshot_url,
        height=height,
        width=width,
        **props
    )
