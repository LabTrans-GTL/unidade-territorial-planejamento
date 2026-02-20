import pandas as pd
from src.interface.consolidation_loader import ConsolidationLoader
from unittest.mock import MagicMock

def test_apply_consolidations_to_dataframe_updates_seats():
    # Setup
    loader = ConsolidationLoader()
    
    # Mock is_executed and get_utps_mapping
    loader.is_executed = MagicMock(return_value=True)
    loader.get_utps_mapping = MagicMock(return_value={"UNIT_UTP": "TARGET_UTP"})
    
    # Mock DataFrame
    data = {
        'cd_mun': [1, 2, 3],
        'nm_mun': ['Mun 1', 'Mun 2', 'Mun 3'],
        'utp_id': ['UNIT_UTP', 'TARGET_UTP', 'TARGET_UTP'],
        'sede_utp': [True, True, False],
        'nm_sede': ['Mun 1', 'Mun 2', 'Mun 2']
    }
    df = pd.DataFrame(data)
    
    # Apply consolidations
    result_df = loader.apply_consolidations_to_dataframe(df)
    
    # Assertions
    # Mun 1 should have moved from UNIT_UTP to TARGET_UTP
    mun1 = result_df[result_df['cd_mun'] == 1].iloc[0]
    assert mun1['utp_id'] == 'TARGET_UTP'
    # Mun 1 was a seat of UNIT_UTP, but now it's just a member of TARGET_UTP
    assert mun1['sede_utp'] == False
    # Mun 1's nm_sede should now be Mun 2 (the seat of TARGET_UTP)
    assert mun1['nm_sede'] == 'Mun 2'
    
    # Mun 2 should remain seat of TARGET_UTP
    mun2 = result_df[result_df['cd_mun'] == 2].iloc[0]
    assert mun2['utp_id'] == 'TARGET_UTP'
    assert mun2['sede_utp'] == True
    assert mun2['nm_sede'] == 'Mun 2'

def test_apply_consolidations_to_geodataframe_updates_seats():
    # Setup - Case for GDF style (NM_MUN uppercase)
    loader = ConsolidationLoader()
    loader.is_executed = MagicMock(return_value=True)
    loader.get_utps_mapping = MagicMock(return_value={"UNIT_UTP": "TARGET_UTP"})
    
    data = {
        'CD_MUN': [1, 2],
        'NM_MUN': ['Mun 1', 'Mun 2'],
        'utp_id': ['UNIT_UTP', 'TARGET_UTP'],
        'sede_utp': [True, True],
        'nm_sede': ['Mun 1', 'Mun 2']
    }
    df = pd.DataFrame(data)
    
    result_df = loader.apply_consolidations_to_dataframe(df)
    
    mun1 = result_df[result_df['CD_MUN'] == 1].iloc[0]
    assert mun1['utp_id'] == 'TARGET_UTP'
    assert mun1['sede_utp'] == False
    assert mun1['nm_sede'] == 'Mun 2'

if __name__ == "__main__":
    test_apply_consolidations_to_dataframe_updates_seats()
    test_apply_consolidations_to_geodataframe_updates_seats()
    print("All tests passed!")
