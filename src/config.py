# src/config.py
from pathlib import Path
import logging
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "01_raw"
INTERIM_DIR = DATA_DIR / "02_intermediate"
MAPS_DIR = DATA_DIR / "04_maps"

FILES = {
    # Inputs (Coloque seus CSVs/Excels aqui)
    "utp_base": RAW_DIR / "UTP_FINAL.xlsx",
    "sede_regic": RAW_DIR / "SEDE+regic.xlsx",
    "rm_composition": RAW_DIR / "Composicao_RM_2024.xlsx",
    "matriz_pessoas": RAW_DIR / "person-matrix-data",
    "impedancias": RAW_DIR / "impedance" / "impedancias_filtradas_2h.csv",
    "shapefiles": RAW_DIR / "shapefiles",

    # Outputs
    "res_destino_principal": INTERIM_DIR / "res_fluxos.csv",
    "mapa_01": MAPS_DIR / "01_INICIAL.png",
    "mapa_05": MAPS_DIR / "05_FUNCIONAL.png",
    "mapa_final": MAPS_DIR / "FINAL_CONSOLIDADO.png",
    "mapa_rm": MAPS_DIR / "06_RM_MAP.png"
}

# Criar pastas automaticamente se não existirem
for folder in [INTERIM_DIR, MAPS_DIR]:
    folder.mkdir(parents=True, exist_ok=True)


def setup_logging(level=logging.INFO):
    """Configura logging centralizado para o projeto."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger("GeoValida")