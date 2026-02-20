# Áreas Operacionais do IBGE

## O que são?

As **áreas operacionais** são códigos especiais atribuídos pelo IBGE a lagos e outras áreas que não são municípios reais, mas que aparecem na malha municipal para fins operacionais.

## Códigos Conhecidos

- **4300001**: Área Operacional "Lagoa Mirim" (Rio Grande do Sul)
- **4300002**: Área Operacional "Lagoa dos Patos" (Rio Grande do Sul)

## Por que filtramos?

Essas áreas:
- Não são municípios reais
- Não possuem população
- Não devem fazer parte das UTPs
- Criavam uma "UTP fantasma" (`UTP_nan`) no pipeline

## Implementação

O filtro foi implementado em:
- `src/core/manager.py`: método `load_from_initialization_json()` (linha ~125)
- `src/core/manager.py`: método `step_0_initialize_data()` fallback path (linha ~196)

O filtro usa a constante:
```python
OPERATIONAL_AREAS = {4300001, 4300002}
```

## Resultado

Com o filtro:
- Step 6: 617 UTPs ✅
- Step 8: 617 UTPs ✅
- Nenhuma UTP fantasma criada ou removida durante cleanup
