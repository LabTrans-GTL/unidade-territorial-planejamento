#!/usr/bin/env python3
import json

with open('data/initialization.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print('=== ANALISE DE DADOS PREENCHIDOS ===')
muns_com_pop = len([m for m in data['municipios'] if m['populacao_2022'] > 0])
muns_com_modal_matriz = len([m for m in data['municipios'] if m['modal_matriz']])
muns_com_imp_06h = len([m for m in data['municipios'] if m['impedancia_06h'] is not None])
muns_com_imp_2h = len([m for m in data['municipios'] if m['impedancia_2h_filtrada'] is not None])

print(f'Municipios com populacao: {muns_com_pop}/{len(data["municipios"])}')
print(f'Municipios com matriz modal: {muns_com_modal_matriz}/{len(data["municipios"])}')
print(f'Municipios com impedancia 06h: {muns_com_imp_06h}/{len(data["municipios"])}')
print(f'Municipios com impedancia 2h: {muns_com_imp_2h}/{len(data["municipios"])}')

print('\n=== EXEMPLO DE MUNICIPIO COM DADOS ===')
for mun in data['municipios'][:100]:
    if mun['modal_matriz'] and len(mun['modal_matriz']) > 0:
        print(f'CD_MUN: {mun["cd_mun"]} - {mun["nm_mun"]}')
        print(f'Modal Matriz: {list(mun["modal_matriz"].keys())}')
        print(f'Impedancia 06h: {mun["impedancia_06h"]}')
        print(f'Impedancia 2h: {mun["impedancia_2h_filtrada"]}')
        print(f'Modais: {mun["modais"]}')
        for modal_name, destinations in list(mun['modal_matriz'].items())[:1]:
            print(f'  {modal_name} - Primeiros 3 destinos:')
            for dest, trips in list(destinations.items())[:3]:
                print(f'    -> {dest}: {trips} viagens')
        break

print('\n=== RESUMO FINAL ===')
print(f'Total de municipios: {len(data["municipios"])}')
print(f'Total de UTPs: {len(data["utps"])}')
print(f'Arquivo gerado com sucesso!')
