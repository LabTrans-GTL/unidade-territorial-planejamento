#!/usr/bin/env python3
import pandas as pd
from src.pipeline.sede_consolidator import SedeConsolidator


def main():
    # Prepare minimal metrics DataFrame: A (409), B (366), C (675)
    df = pd.DataFrame([
        {'cd_mun_sede': 409, 'nm_sede':'OrigemA', 'regic':'centro local', 'tem_aeroporto': False, 'turismo': ''},
        {'cd_mun_sede': 366, 'nm_sede':'InterB', 'regic':'centro local', 'tem_aeroporto': False, 'turismo': ''},
        {'cd_mun_sede': 675, 'nm_sede':'FinalC', 'regic':'capital regional a', 'tem_aeroporto': True, 'turismo': ''}
    ])

    # Approved candidate: B -> C (366 -> 675)
    candidates = [
        {
            'sede_origem': 366,
            'sede_destino': 675,
            'utp_origem': 'UTP_366',
            'utp_destino': 'UTP_675',
            'score_origem': 0,
            'score_destino': 1,
            'tempo_viagem_h': 1.0
        }
    ]

    # Rejected tie: A -> B (409 -> 366) recorded using sede (seed) of UTP destino
    rejected_candidates = [
        {
            'sede_origem': 409,
            'sede_destino': 366,
            'utp_origem': 'UTP_409',
            'utp_destino': 'UTP_366',
            'nm_origem': 'OrigemA',
            'nm_destino': 'InterB',
            'motivo_rejeicao': 'Ambos score=0, mas destino REGIC pior/igual (rank 11 >= 11)'
        }
    ]

    # Provide a minimal graph stub implementing get_municipality_utp
    class DummyGraph:
        def __init__(self):
            self._map = {409: 'UTP_409', 366: 'UTP_366', 675: 'UTP_675'}
        def get_municipality_utp(self, mun_id):
            return self._map.get(int(mun_id))

    consolidator = SedeConsolidator(DummyGraph(), None, None)
    consolidator.rejected_candidates = rejected_candidates

    new_candidates = consolidator._apply_transitive_consolidation(candidates.copy(), df)

    # Print resulting candidates for inspection
    print('New candidates result:')
    for c in new_candidates:
        print(c)

    # Check if A was approved to final (675)
    added = [c for c in new_candidates if c.get('transitive') and int(c['sede_origem']) == 409 and int(c['sede_destino']) == 675]

    print('Transitive approvals found (to final):', len(added))
    for a in added:
        print(a)

    assert len(added) == 1, 'Transitive approval not created as expected'


if __name__ == '__main__':
    main()
