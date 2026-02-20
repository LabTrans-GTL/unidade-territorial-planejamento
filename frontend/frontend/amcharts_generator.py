"""
amCharts Map Generator — Optimized Edition
Generates HTML files with amCharts 5 maps for each pipeline step.

Performance design:
- GeoJSON is served EXTERNALLY (/municipalities_v3.geojson) — HTML stays small
- MAP_DATA contains only flat fields per municipality (~70 bytes each, not ~1KB HTML)
- A single JS tooltip template uses {dataItem.dataContext.field} placeholders
- Result: ~400 KB HTML instead of 5+ MB
"""
import json
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# ── Color Palette ──────────────────────────────────────────────────────────────
COLOR_MAP: dict[int, str] = {
    0: "#CCCCCC",
    1: "#2D6A4F",
    2: "#52B788",
    3: "#95D5B2",
    4: "#FFCF00",
    5: "#FF8C00",
    6: "#E63946",
    7: "#6A0DAD",
}

COLOR_LABELS: dict[int, str] = {
    1: "Nível 1 (Capital)",
    2: "Nível 2",
    3: "Nível 3",
    4: "Nível 4",
    5: "Nível 5",
    6: "Nível 6",
    7: "Nível 7 (Menor)",
}

SNAPSHOT_FILES: dict[str, str] = {
    "step1": "snapshot_step1_initial.json",
    "step5": "snapshot_step5_post_unitary.json",
    "step6": "snapshot_step6_sede_consolidation.json",
    "step8": "snapshot_step8_final.json",
}

# ── amCharts tooltip template (single JS string, reused for all polygons) ──────
# Placeholders reference fields emitted in MAP_DATA via dataItem.dataContext.*
# This pattern avoids pre-rendering HTML per polygon (saves ~5 MB).
TOOLTIP_TEMPLATE = """
<div style="background:#fff;padding:12px 14px;border-radius:8px;box-shadow:0 4px 16px rgba(0,0,0,0.18);color:#222;font-size:12px;min-width:190px;font-family:'Segoe UI',system-ui,sans-serif;line-height:1.6;">
  <div style="font-size:14px;font-weight:700;margin-bottom:6px;color:#071D41">{nm}</div>
  <div style="margin-bottom:6px" id="_badge_{code}"></div>
  <table style="width:100%;border-collapse:collapse">
    <tr><td style="color:#888;padding:2px 0;width:38%">Código</td><td style="font-weight:500">{code}</td></tr>
    <tr><td style="color:#888;padding:2px 0">UTP</td><td style="font-weight:700;color:#071D41">{utp}</td></tr>
    <tr><td style="color:#888;padding:2px 0">RM</td><td>{rm}</td></tr>
  </table>
</div>
""".strip()


class AmChartsMapGenerator:
    """Generate amCharts 5 HTML maps from snapshot data."""

    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.snapshots_dir = data_root / "03_processed"

    # ── Data Loading ───────────────────────────────────────────────────────────

    def load_snapshot(self, step: str) -> Optional[dict]:
        filename = SNAPSHOT_FILES.get(step)
        if not filename:
            logger.error(f"Unknown step key: {step!r}")
            return None
        path = self.snapshots_dir / filename
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading snapshot {step!r}: {e}")
            return None

    # ── Data Preparation ───────────────────────────────────────────────────────

    def build_map_data(self, snapshot: dict) -> tuple[list[dict], set[int]]:
        """Build lean MAP_DATA list and set of active color IDs.

        Each record is ~70 bytes (flat fields), not ~1 KB (pre-rendered HTML).
        The tooltip HTML is assembled at runtime by the JS template.
        """
        coloring: dict = snapshot.get("coloring", {})
        nodes: dict = snapshot.get("nodes", {})

        map_data: list[dict] = []
        active_colors: set[int] = set()

        for codibge in set(coloring.keys()) | set(nodes.keys()):
            node = nodes.get(codibge, {})
            color_id: int = coloring.get(codibge, 0)
            active_colors.add(color_id)

            rm_raw: str = node.get("regiao_metropolitana", "SEM_RM")

            map_data.append({
                "id":   str(codibge),
                "fill": COLOR_MAP.get(color_id, "#CCCCCC"),
                # Flat fields — consumed by JS tooltip template
                "nm":   node.get("name", ""),
                "utp":  node.get("utp_id", "SEM_UTP"),
                "code": str(codibge),
                "sede": 1 if node.get("sede_utp", False) else 0,
                "rm":   rm_raw if rm_raw != "SEM_RM" else "—",
            })

        return map_data, active_colors

    # ── Legend HTML ────────────────────────────────────────────────────────────

    def build_legend_html(self, active_colors: set[int]) -> str:
        items = sorted(
            [(cid, COLOR_MAP[cid], COLOR_LABELS[cid])
             for cid in active_colors if cid in COLOR_LABELS],
            key=lambda x: x[0],
        )
        if not items:
            return ""

        rows = "".join(
            f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0">'
            f'<div style="width:13px;height:13px;border-radius:3px;background:{color};'
            f'flex-shrink:0;border:1px solid rgba(0,0,0,0.12)"></div>'
            f'<span style="font-size:11px;color:#444">{label}</span></div>'
            for _, color, label in items
        )
        return (
            '<div id="legend" style="position:absolute;bottom:12px;right:12px;'
            'background:rgba(255,255,255,0.96);backdrop-filter:blur(8px);'
            'border-radius:8px;padding:10px 12px;box-shadow:0 2px 12px rgba(0,0,0,0.12);'
            'font-family:\'Segoe UI\',system-ui,sans-serif;z-index:100;min-width:155px;">'
            '<div style="font-size:10px;font-weight:700;color:#071D41;margin-bottom:6px;'
            'text-transform:uppercase;letter-spacing:0.6px">Legenda UTP</div>'
            f'{rows}</div>'
        )

    # ── HTML Generation ────────────────────────────────────────────────────────

    def generate_html(self, step: str, title: str) -> Optional[str]:
        """Generate optimized amCharts 5 HTML (~400 KB, not 5+ MB).

        Key decisions:
        - GeoJSON loaded externally: /municipalities_v3.geojson (Reflex asset)
        - MAP_DATA: flat fields only (~70 bytes/municipality)
        - Single TOOLTIP_TEMPLATE with {field} placeholders, rendered by amCharts
        - amCharts native ZoomControl (no decorative Reflex buttons needed)
        - postMessage bridge for optional parent→iframe control
        """
        snapshot = self.load_snapshot(step)
        if not snapshot:
            return None

        map_data, active_colors = self.build_map_data(snapshot)
        legend_html = self.build_legend_html(active_colors)

        # Compact JSON serialization (no whitespace)
        map_data_json = json.dumps(map_data, ensure_ascii=False, separators=(",", ":"))

        # Escape braces in the tooltip template so Python's f-string doesn't
        # interpret them, but keep {field} placeholders for amCharts JS.
        # Strategy: build the JS tooltip string as a raw literal in the f-string
        # by using a variable reference instead of inline literal.
        tooltip_tpl = TOOLTIP_TEMPLATE  # single-line reference avoids f-string conflict

        return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<script src="https://cdn.amcharts.com/lib/5/index.js"></script>
<script src="https://cdn.amcharts.com/lib/5/map.js"></script>
<script src="https://cdn.amcharts.com/lib/5/themes/Animated.js"></script>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
html,body{{width:100%;height:100%;overflow:hidden;font-family:'Segoe UI',system-ui,sans-serif;background:#f0f4f8}}
#chartdiv{{width:100%;height:100%;position:relative}}
#loader{{
  position:absolute;inset:0;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:12px;
  background:rgba(240,244,248,0.95);z-index:999;transition:opacity 0.4s ease;
}}
#loader.hidden{{opacity:0;pointer-events:none}}
.spinner{{
  width:36px;height:36px;border:3px solid #e0e7ef;border-top-color:#071D41;
  border-radius:50%;animation:spin 0.8s linear infinite;
}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
.loader-txt{{font-size:13px;color:#4a6080;font-weight:500;letter-spacing:0.3px}}
</style>
</head>
<body>
<div id="chartdiv">
  <div id="loader">
    <div class="spinner"></div>
    <div class="loader-txt">Carregando mapa...</div>
  </div>
</div>
{legend_html}
<script>
// ── Pre-processed municipality data (generated server-side, lean fields only) ──
const MAP_DATA = {map_data_json};

// ── Tooltip HTML template (single definition, reused for all ~5500 polygons) ──
// amCharts resolves {{field}} placeholders from dataItem.dataContext at runtime.
const TOOLTIP_TPL = `{tooltip_tpl}`;

// ── amCharts 5 root ────────────────────────────────────────────────────────────
const root = am5.Root.new("chartdiv");
root.setThemes([am5themes_Animated.new(root)]);

// ── Map chart — Mercator, centered on Brazil ───────────────────────────────────
const chart = root.container.children.push(
  am5map.MapChart.new(root, {{
    panX: "translateX",
    panY: "translateY",
    projection: am5map.geoMercator(),
    homeGeoPoint: {{ longitude: -51.9, latitude: -14 }},
    homeZoomLevel: 1.8,
    minZoomLevel: 1,
    maxZoomLevel: 64,
  }})
);

// ── Polygon Series ─────────────────────────────────────────────────────────────
const polygonSeries = chart.series.push(
  am5map.MapPolygonSeries.new(root, {{
    geoJSON: am5.net.load("/municipalities_v3.geojson", chart),
    calculateAggregates: false,
  }})
);

// Tooltip object — transparent background, HTML rendered from template
const tooltip = am5.Tooltip.new(root, {{
  getFillFromSprite: false,
  labelHTML: TOOLTIP_TPL,
  background: am5.RoundedRectangle.new(root, {{
    fill: am5.color(0xffffff),
    fillOpacity: 0,
    strokeOpacity: 0,
  }}),
}});

// Polygon defaults
polygonSeries.mapPolygons.template.setAll({{
  fillField: "fill",
  interactive: true,
  toggleKey: "active",
  strokeWidth: 0.4,
  stroke: am5.color(0x000000),
  strokeOpacity: 0.2,
  cursorOverStyle: "pointer",
  tooltip: tooltip,
}});

// Hover state — government blue
polygonSeries.mapPolygons.template.states.create("hover", {{
  fill: am5.color(0x1A3A6E),
  stroke: am5.color(0x071D41),
  strokeWidth: 1.5,
  strokeOpacity: 0.9,
}});

// Active (click) state — Amarelo Brasil
polygonSeries.mapPolygons.template.states.create("active", {{
  fill: am5.color(0xEACD04),
  stroke: am5.color(0x071D41),
  strokeWidth: 2.0,
  strokeOpacity: 1.0,
}});

// Bind data (pure in-memory — very fast for 5000+ items)
polygonSeries.data.setAll(MAP_DATA);

// ── Native Zoom Control ───────────────────────────────────────────────────────
const zoomControl = chart.set("zoomControl", am5map.ZoomControl.new(root, {{}}));
zoomControl.homeButton.set("visible", true);

// ── Loading overlay ────────────────────────────────────────────────────────────
const loader = document.getElementById("loader");

polygonSeries.events.once("datavalidated", () => {{
  loader.classList.add("hidden");
  setTimeout(() => {{ loader.style.display = "none"; }}, 450);
}});

// Fallback hide after 20s
setTimeout(() => {{
  loader.classList.add("hidden");
  setTimeout(() => {{ loader.style.display = "none"; }}, 450);
}}, 20000);

// ── Entrance animation ─────────────────────────────────────────────────────────
chart.appear(1200, 150);
polygonSeries.appear();

// ── postMessage bridge ─────────────────────────────────────────────────────────
// Parent Reflex page can control the map via:
//   document.getElementById("amcharts-map-frame").contentWindow.postMessage(...)
window.addEventListener("message", (event) => {{
  const d = event.data;
  if (!d || typeof d !== "object") return;
  if (d.action === "homeView") chart.goHome(600);
  if (d.action === "zoomIn")   chart.zoomIn();
  if (d.action === "zoomOut")  chart.zoomOut();
}});
</script>
</body>
</html>"""

    # ── Save ───────────────────────────────────────────────────────────────────

    def generate_and_save(self, step: str, title: str, output_path: Path) -> bool:
        html = self.generate_html(step, title)
        if not html:
            return False
        try:
            output_path.write_text(html, encoding="utf-8")
            size_kb = output_path.stat().st_size // 1024
            logger.info(f"✅ amCharts map saved: {output_path.name} ({size_kb} KB)")
            return True
        except Exception as e:
            logger.error(f"Error saving HTML to {output_path}: {e}")
            return False
