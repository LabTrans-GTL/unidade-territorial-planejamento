
"""
Script to pre-compute flow popups for all municipalities for each pipeline step.
Generates step-specific popup files to ensure correct UTP assignments are displayed.
"""
import sys
import pandas as pd
import json
import logging
from pathlib import Path
from tqdm import tqdm

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.config import setup_logging
from src.interface.snapshot_loader import SnapshotLoader
from src.interface.flow_utils import (
    get_top_destinations_for_municipality, format_flow_popup_html,
    get_municipality_total_flow, load_idh_pib_data, get_idh_for_municipality
)

logger = logging.getLogger(__name__)

def load_impedance_data():
    """Loads impedance data for travel time calculations."""
    try:
        impedance_path = PROJECT_ROOT / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
        if impedance_path.exists():
            df_impedance = pd.read_csv(impedance_path, sep=';', encoding='latin-1')
            df_impedance = df_impedance.dropna(axis=1, how='all')
            df_impedance = df_impedance.rename(columns={
                'COD_IBGE_ORIGEM_1': 'origem_6',
                'COD_IBGE_DESTINO_1': 'destino_6',
                'Tempo': 'tempo_horas'
            })
            # Convert tempo_horas to float
            df_impedance['tempo_horas'] = (
                df_impedance['tempo_horas'].astype(str).str.replace(',', '.').astype(float)
            )
            # Ensure 6-digit keys are int
            df_impedance['origem_6'] = pd.to_numeric(df_impedance['origem_6'], errors='coerce').fillna(0).astype(int)
            df_impedance['destino_6'] = pd.to_numeric(df_impedance['destino_6'], errors='coerce').fillna(0).astype(int)
            logger.info(f"Loaded {len(df_impedance)} impedance pairs for popup times")
            return df_impedance
        else:
            logger.warning(f"Impedance file not found: {impedance_path}")
            return None
    except Exception as e:
        logger.warning(f"Could not load impedance data for popups: {e}")
        return None


def precompute_for_step(step_key: str, output_filename: str, df_impedance=None):
    """
    Pre-compute popups for a specific pipeline step using snapshot data.
    
    Args:
        step_key: Snapshot key ('step1', 'step5', 'step6', 'step8')
        output_filename: Output JSON filename (e.g., 'flow_popups_step1.json')
        df_impedance: Optional impedance DataFrame for travel times
        
    Returns:
        Number of popups generated, or -1 on error
    """
    logger.info(f"Computing popups for {step_key}...")
    
    # Load snapshot data for this step
    snapshot_loader = SnapshotLoader()
    df_municipios = snapshot_loader.get_complete_dataframe_with_flows(step_key)
    
    if df_municipios.empty:
        logger.warning(f"No data found for {step_key}, skipping...")
        return -1
    
    # Load IDH/PIB data once for all municipalities in this step
    _pib_by_cd_mun: dict = {}
    try:
        _, _pib_by_cd_mun = load_idh_pib_data()
        logger.info(f"IDH/PIB loaded for popup pre-computation")
    except Exception as e:
        logger.warning(f"Could not load IDH/PIB data: {e}")
    
    # Setup lookup dictionary
    df_lookup = df_municipios.set_index('cd_mun') if 'cd_mun' in df_municipios.columns else pd.DataFrame()
    
    popups = {}
    count = 0
    
    for idx, row in tqdm(df_municipios.iterrows(), total=len(df_municipios), desc=f"Computing {step_key}"):
        try:
            cd_mun_val = row.get('CD_MUN') if 'CD_MUN' in row else row.get('cd_mun')
            if not cd_mun_val: 
                continue
            
            cd_mun = str(cd_mun_val)
            nm_mun = row.get('NM_MUN', row.get('nm_mun', 'Desconhecido'))
            utp_id = str(row.get('utp_id', ''))
            
            # Lookup detailed data (nested dicts/lists for modals)
            mun_data = {}
            if cd_mun in df_lookup.index:
                entry = df_lookup.loc[cd_mun]
                if isinstance(entry, pd.DataFrame):
                    entry = entry.iloc[0]
                mun_data = entry.to_dict()
                mun_data['cd_mun'] = cd_mun  # Ensure ID is present
                # Preserve modal_matriz
                if 'modal_matriz' in entry:
                    mun_data['modal_matriz'] = entry['modal_matriz']
            else:
                # Fallback to current row data converted to dict
                mun_data = row.to_dict()
            
            # Calculate top destinations using the snapshot's UTP assignments
            top_destinations = get_top_destinations_for_municipality(
                mun_data, df_municipios, top_n=5, df_impedance=df_impedance,
                pib_by_cd_mun=_pib_by_cd_mun
            )
            
            # Extract fields
            regiao_metropolitana = row.get('regiao_metropolitana', mun_data.get('regiao_metropolitana', '-'))
            regic = row.get('regic', mun_data.get('regic', '-'))
            populacao = row.get('populacao_2022', mun_data.get('populacao_2022', 0))
            uf = row.get('uf', mun_data.get('uf', ''))
            
            total_viagens = get_municipality_total_flow(mun_data)
            
            # Lookup IDH and PIB
            idh_val = get_idh_for_municipality(nm_mun, uf)
            pib_val = _pib_by_cd_mun.get(str(cd_mun)) or _pib_by_cd_mun.get(cd_mun)
            
            # Generate HTML
            html = format_flow_popup_html(
                nm_mun=nm_mun, 
                cd_mun=cd_mun, 
                utp_id=utp_id, 
                top_destinations=top_destinations,
                regiao_metropolitana=regiao_metropolitana,
                regic=regic,
                populacao=populacao,
                total_viagens=total_viagens,
                uf=uf,
                idh=idh_val,
                pib_mil_reais=pib_val,
            )
            
            popups[cd_mun] = html
            count += 1
            
        except Exception as e:
            logger.warning(f"Error computing popup for {row.get('nm_mun', 'unknown')} in {step_key}: {e}")
            continue
    
    # Save to JSON
    output_dir = PROJECT_ROOT / "data" / "04_maps"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_filename
    
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(popups, f, ensure_ascii=False)
        logger.info(f"✅ Saved {count} popups for {step_key} to {output_path}")
        return count
    except Exception as e:
        logger.error(f"❌ Failed to save popups for {step_key}: {e}")
        return -1


def main():
    logger.info("Starting Flow Popups Pre-computation for all pipeline steps...")
    
    # Load impedance data once
    df_impedance = load_impedance_data()
    
    # Define steps to generate popups for
    steps = [
        ('step1', 'flow_popups_step1.json'),
        ('step5', 'flow_popups_step5.json'),
        ('step6', 'flow_popups_step6.json'),
        ('step8', 'flow_popups_step8.json'),
    ]
    
    results = {}
    for step_key, filename in steps:
        count = precompute_for_step(step_key, filename, df_impedance)
        results[step_key] = count
    
    # Summary
    logger.info("\n" + "="*50)
    logger.info("POPUP GENERATION SUMMARY")
    logger.info("="*50)
    
    total_success = 0
    for step_key, count in results.items():
        if count > 0:
            logger.info(f"✅ {step_key}: {count} popups")
            total_success += 1
        else:
            logger.warning(f"⚠️ {step_key}: FAILED or SKIPPED")
    
    logger.info("="*50)
    
    # Return success if at least one step succeeded
    return 0 if total_success > 0 else 1


if __name__ == "__main__":
    setup_logging()
    sys.exit(main())
