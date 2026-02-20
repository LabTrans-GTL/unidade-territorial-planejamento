#!/usr/bin/env python3
import pandas as pd
from pathlib import Path

proj = Path(__file__).resolve().parent.parent
imp_path = proj / 'data' / '01_raw' / 'impedance' / 'impedancias_filtradas_2h.csv'
flows_path = proj / 'data' / 'analysis_flows_to_sedes.csv'

orig = 4300406
dest = 4316907
orig6 = int(orig) // 10
dest6 = int(dest) // 10

print('Impedance file:', imp_path.exists(), imp_path)
if imp_path.exists():
    try:
        df = pd.read_csv(imp_path, sep=';', encoding='latin-1', low_memory=False)
        # normalize columns
        df = df.rename(columns={
            'COD_IBGE_ORIGEM_1': 'origem_6',
            'COD_IBGE_DESTINO_1': 'destino_6',
            'Tempo': 'tempo_horas'
        })
        df['origem_6'] = pd.to_numeric(df['origem_6'], errors='coerce').fillna(0).astype(int)
        df['destino_6'] = pd.to_numeric(df['destino_6'], errors='coerce').fillna(0).astype(int)
        # find match
        match = df[(df['origem_6']==orig6) & (df['destino_6']==dest6)]
        print('Impedance matches:', len(match))
        if not match.empty:
            # print unique tempos
            tempos = match['tempo_horas'].astype(str).str.replace(',','.').astype(float).unique()
            print('Tempos (h):', tempos)
            print('Any <=2h?', any(t<=2.0 for t in tempos))
    except Exception as e:
        print('Error reading impedance:', e)
else:
    print('Impedance file not found')

print('\nChecking flows file (analysis_flows_to_sedes.csv) existence...')
print('Flows file:', flows_path.exists(), flows_path)
if flows_path.exists():
    try:
        # read in chunks to avoid memory
        found = False
        for chunk in pd.read_csv(flows_path, chunksize=200000, low_memory=False):
            # try common column names
            col_opts = [('mun_origem','mun_destino','tempo_viagem'),('mun_origem','mun_destino','tempo_ate_destino_h'),('origem','destino','tempo')]
            cols = chunk.columns.tolist()
            if 'mun_origem' in cols and 'mun_destino' in cols:
                c = chunk[(chunk['mun_origem']==orig) & (chunk['mun_destino']==dest)]
                if not c.empty:
                    print('Found flows rows count:', len(c))
                    # print sample columns
                    for col in ['tempo_viagem','tempo_ate_destino_h','viagens']:
                        if col in c.columns:
                            print(col, c[col].head(3).tolist())
                    found = True
                    break
        if not found:
            print('No direct flow rows found in flows file for origin->dest')
    except Exception as e:
        print('Error reading flows:', e)
else:
    print('Flows file not found')
