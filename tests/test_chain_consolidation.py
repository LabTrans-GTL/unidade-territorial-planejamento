#!/usr/bin/env python3
"""
Script de teste para validar as correções de consolidação
"""
import sys
from pathlib import Path

# Adicionar raiz ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.interface.consolidation_loader import ConsolidationLoader
import pandas as pd

print("=" * 80)
print("TESTE 1: Função de Resolução de Cadeia")
print("=" * 80)

loader = ConsolidationLoader()

# Teste com mapeamento simulado
test_mapping = {'131': '152', '152': '672', '999': '1000'}

print("\nMapeamento de teste:")
for k, v in test_mapping.items():
    print(f"  {k} -> {v}")

print("\nResolução de cadeias:")
print(f"  131 -> {loader._resolve_mapping_chain('131', test_mapping)} (esperado: 672)")
print(f"  152 -> {loader._resolve_mapping_chain('152', test_mapping)} (esperado: 672)")
print(f"  999 -> {loader._resolve_mapping_chain('999', test_mapping)} (esperado: 1000)")
print(f"  888 -> {loader._resolve_mapping_chain('888', test_mapping)} (esperado: 888)")

# Validar
assert loader._resolve_mapping_chain('131', test_mapping) == '672', "Falha: 131 não resolveu para 672"
assert loader._resolve_mapping_chain('152', test_mapping) == '672', "Falha: 152 não resolveu para 672"
assert loader._resolve_mapping_chain('888', test_mapping) == '888', "Falha: 888 deveria retornar 888"

print("\n✅ Teste de cadeia passou!")

print("\n" + "=" * 80)
print("TESTE 2: Consolidação Real de Quaraí")
print("=" * 80)

# Verificar dados reais
print(f"\nConsolidações existentes: {loader.result['total_consolidations']}")
print(f"Status: {loader.result['status']}")

mapping = loader.get_utps_mapping()

if '131' in mapping:
    print(f"\nCadeia de Quaraí:")
    print(f"  UTP 131 (Quaraí) -> {mapping['131']}")
    
    if '152' in mapping:
        print(f"  UTP 152 -> {mapping['152']}")
    
    resolved = loader._resolve_mapping_chain('131', mapping)
    print(f"\n  Resolução final: 131 -> {resolved}")
    
    if resolved == '672':
        print("\n✅ Quaraí resolve corretamente para UTP 672!")
    else:
        print(f"\n⚠️ Quaraí resolveu para {resolved}, esperado 672")
else:
    print("\n⚠️ UTP 131 (Quaraí) não está no mapeamento de consolidações")

print("\n" + "=" * 80)
print("TESTE 3: DataFrame com Consolidação em Cadeia")
print("=" * 80)

# Simular DataFrame com Quaraí
df_test = pd.DataFrame([
    {'cd_mun': 4315305, 'nm_mun': 'Quaraí', 'utp_id': '131', 'sede_utp': True, 'nm_sede': 'Quaraí'},
    {'cd_mun': 4317103, 'nm_mun': 'Sant Ana do Livramento', 'utp_id': '152', 'sede_utp': True, 'nm_sede': 'Sant Ana do Livramento'},
    {'cd_mun': 4323002, 'nm_mun': 'Viamão', 'utp_id': '672', 'sede_utp': True, 'nm_sede': 'Viamão'},
])

print("\nDataFrame original:")
print(df_test[['nm_mun', 'utp_id', 'sede_utp']].to_string(index=False))

# É possível que a consolidação já tenha sido aplicada e precise ser reprocessada
# Vamos ver o resultado
df_result = loader.apply_consolidations_to_dataframe(df_test)

print("\nDataFrame após consolidação:")
print(df_result[['nm_mun', 'utp_id', 'sede_utp', 'nm_sede']].to_string(index=False))

# Validar Quaraí
quarai = df_result[df_result['cd_mun'] == 4315305].iloc[0]
print(f"\nQuaraí:")
print(f"  UTP: {quarai['utp_id']}")
print(f"  É sede?: {quarai['sede_utp']}")
print(f"  Nome da sede: {quarai['nm_sede']}")

if quarai['utp_id'] == '672' and not quarai['sede_utp']:
    print("\n✅ Quaraí está na UTP correta e não é mais sede!")
else:
    print(f"\n⚠️ Quaraí - UTP: {quarai['utp_id']} (esperado: 672), É sede: {quarai['sede_utp']} (esperado: False)")

print("\n" + "=" * 80)
print("TESTE COMPLETO!")
print("=" * 80)
