# Resumo do Projeto GeoValida: Pipeline de Consolidação de UTPs

## 📋 Visão Geral

Este documento descreve o processo de validação e consolidação das Unidades Territoriais de Planejamento (UTPs) no projeto GeoValida, detalhando a evolução de V7 para V8 e V9, incluindo as regras de validação e a lógica de consolidação implementada.

---

## 🔍 Contexto: Problemas Identificados na V7

A **V7** é a configuração inicial das UTPs que foi fornecida como base de dados. Durante a análise, foram identificadas **violações críticas** em duas regras fundamentais:

### ❌ **Regra 1: Integridade de Região Metropolitana (RM)**

> **Definição**: Todos os municípios de uma UTP devem **ou** pertencer todos à mesma RM **ou** nenhum município pode pertencer a qualquer RM.

**Problemas Encontrados:**
- UTPs contendo municípios de **múltiplas RMs diferentes**
- UTPs com **mistura de municípios** pertencentes a uma RM e municípios que não pertencem a nenhuma RM

**Exemplo de Erro:**
```
UTP 42:
  - Município A → RM Metropolitana de São Paulo
  - Município B → RM Metropolitana de Campinas  ❌ (duas RMs diferentes)
  - Município C → Sem RM                        ❌ (mix RM/Não-RM)
```

### ❌ **Regra 2: Contiguidade Geográfica**

> **Definição**: Todos os municípios de uma UTP devem ser geograficamente **contíguos**, ou seja, formarem uma região conectada sem municípios isolados.

**Problemas Encontrados:**
- UTPs com municípios **geograficamente isolados** (ilhas)
- UTPs fragmentadas em múltiplos **componentes desconectados**

**Exemplo de Erro:**
```
UTP 73:
  - Componente 1: Municípios [A, B, C] (conectados)
  - Componente 2: Município [D]         ❌ (isolado geograficamente)
```

### 📊 Relatório de Validação

Os erros da V7 foram documentados no arquivo:
```
📁 data/03_validation/v7_validation_report.xlsx
```

Este relatório contém:
- **Resumo**: Quantidade total de erros por categoria
- **Erros_RM**: Detalhamento de todas as violações de RM
- **Erros_Contiguidade**: Detalhamento de todas as violações de contiguidade

Para cada erro, o relatório especifica:
- ID da UTP
- Tipo de erro
- Descrição detalhada
- Lista de municípios afetados
- Códigos municipais

---

## ✅ V8: Correção e Base do Pipeline

A **V8** é a versão **corrigida** da V7, onde todas as violações foram manualmente ajustadas para respeitar as regras de RM e contiguidade.

### Propósito da V8:
1. **Servir como entrada válida** para o pipeline de consolidação
2. **Garantir que todas UTPs** respeitam as regras fundamentais
3. **Possibilitar consolidações automatizadas** sem propagar erros

### Características da V8:
- ✅ Todas UTPs respeitam regra de RM
- ✅ Todas UTPs são geograficamente contíguas
- ✅ Pronta para ser processada pelo pipeline

---

## 🚀 V9: Consolidação Automatizada

A **V9** é gerada a partir da V8 através de um **pipeline automatizado de consolidação**, cujo objetivo é:

> **Consolidar UTPs unitárias** (com apenas 1 município) em UTPs maiores, respeitando critérios funcionais e territoriais.

### 🎯 Objetivos da Consolidação:

1. **Reduzir UTPs unitárias** para criar unidades de planejamento mais robustas
2. **Respeitar fluxos funcionais** entre municípios (comutação, serviços, etc.)
3. **Manter integridade de RMs** durante todas as consolidações
4. **Garantir contiguidade geográfica** após cada fusão

---

## 🔄 Lógica do Pipeline de Consolidação (V8 → V9)

O pipeline implementado no arquivo [`consolidator.py`](file:///c:/Users/vinicios.buzzi/buzzi/geovalida/src/pipeline/consolidator.py) segue uma **sequência hierárquica de regras**:

### **Passo 1: Identificação de UTPs Unitárias**

Identifica todas as UTPs que possuem apenas **1 município** e que são candidatas à consolidação.

---

### **Passo 2: Consolidação por RM (Funcional com Fluxo)**

**Regra aplicada:** UTPs unitárias **dentro de uma RM** são consolidadas com UTPs vizinhas da **mesma RM**.

**Critério de escolha:**
- Identifica todas as UTPs vizinhas (adjacentes geograficamente) **da mesma RM**
- Seleciona a UTP vizinha com **maior fluxo total** (somatória de fluxos de todos os municípios)
- Se houver empate no fluxo, aplica critério de desempate (ex: maior população)

**Exemplo:**
```
Município Unitário M1 (RM São Paulo):
  - Fluxo para UTP 10 (RM SP): 5.000 pessoas
  - Fluxo para UTP 11 (RM SP): 8.000 pessoas  ← ESCOLHIDA
  
Resultado: M1 é incorporado à UTP 11
```

---

### **Passo 3: Consolidação Sem RM (Funcional com Fluxo - Recursiva)**

**Regra aplicada:** UTPs unitárias **sem RM** são consolidadas com UTPs vizinhas **sem RM**.

**Algoritmo (BFS - Busca em Largura):**
1. Calcula o **fluxo total** de cada UTP unitária (soma de todos os fluxos de saída)
2. Ordena UTPs unitárias por **maior fluxo total** (prioridade)
3. Para cada UTP unitária (em ordem decrescente de fluxo):
   - Identifica UTPs vizinhas **sem RM**
   - Seleciona a vizinha com **maior fluxo total**
   - Consolida e **atualiza o grafo**
4. **Repete recursivamente** até não haver mais consolidações possíveis

**Vantagem:** Prioriza consolidações de UTPs mais "ativas" funcionalmente primeiro.

---

### **Passo 4: Consolidação por REGIC (Territorial)**

**Regra aplicada:** UTPs unitárias **sem fluxo identificado** são consolidadas usando **hierarquia urbana (REGIC)**.

**Critério REGIC:**
- REGIC classifica municípios em níveis de relevância urbana:
  - **Metrópole**: Nível mais alto
  - **Capital Regional**: Nível intermediário
  - **Centro Sub-regional, Centro de Zona, Centro Local**: Níveis inferiores

**Algoritmo:**
1. Para cada UTP unitária sem consolidação por fluxo:
   - Identifica UTPs vizinhas geograficamente
   - Seleciona a UTP vizinha cuja **sede possui maior classificação REGIC**
   - Em caso de empate, aplica critérios adicionais:
     - Menor **distância geográfica** à sede
     - Maior **fronteira compartilhada**

**Exemplo:**
```
Município Unitário M2 (sem fluxo identificado):
  - Vizinha UTP 20 (Sede: Centro Sub-regional)
  - Vizinha UTP 21 (Sede: Capital Regional)  ← ESCOLHIDA (maior REGIC)
  
Resultado: M2 é incorporado à UTP 21
```

---

## 📂 Estrutura de Dados

### Dados de Entrada (V8):
```
data/01_raw/
├── v7_base 2(br_municipios_2024).csv   # V7 (com erros)
├── Composicao_RM_2024.xlsx             # Composição de RMs
└── shapefiles/
    └── BR_Municipios_2024.shp          # Geometrias municipais
```

### Dados Intermediários:
```
data/02_processed/
├── flow_matrix.csv                      # Matriz de fluxos entre municípios
└── adjacency_graph.pkl                  # Grafo de adjacências geográficas
```

### Dados de Saída (V9):
```
data/03_output/
├── v9_consolidated.csv                  # UTPs após consolidação
└── consolidation_log.json               # Log de todas consolidações
```

---

## 🔧 Ferramentas e Scripts

### **Script de Validação V7:**
```bash
python scripts/validate_v7.py
```
- Gera relatório de erros em `data/03_validation/v7_validation_report.xlsx`

### **Pipeline Principal (V8 → V9):**
```bash
python main.py
```
- Executa toda a sequência de consolidação
- Gera V9 e logs

### **Módulo Consolidador:**
```python
from src.pipeline.consolidator import UTPConsolidator
```
- Classe principal com toda a lógica de consolidação

---

## 🔮 Próximas Etapas

### **Planejadas:**

1. **Validação Automatizada de V9**
   - Verificar se V9 mantém regras de RM e contiguidade
   - Comparar métricas antes/depois da consolidação

2. **Dashboard Interativo**
   - Visualização de mapas V7 vs V8 vs V9
   - Comparação de métricas (população, PIB, área)
   - Exploração de consolidações individuais

3. **Refinamento de Critérios**
   - Ajustar pesos de fluxo vs REGIC
   - Incorporar critérios adicionais (ex: capacidade fiscal)

4. **Documentação de Casos Especiais**
   - Documentar decisões para UTPs não consolidáveis
   - Criar workflow de revisão manual

### **Desafios Conhecidos:**

- **UTPs Insulares**: Municípios isolados por água (ex: Fernando de Noronha)
- **Fronteiras Estaduais**: Consolidações que cruzam estados (permitir ou não?)
- **Critérios Conflitantes**: Casos onde fluxo e REGIC apontam direções diferentes

---

## 📚 Referências

- **REGIC 2018**: Regiões de Influência das Cidades (IBGE)
- **Composição de RMs**: Legislação estadual (atualizada em 2024)
- **Fluxos Pendulares**: Censo IBGE 2010 + ACS BigData

---

## 👥 Autores e Contato

**Projeto:** GeoValida  
**Responsável:** Vinicios Buzzi  
**Data:** Janeiro 2026

---

## 📝 Histórico de Versões

| Versão | Data       | Descrição                                      |
|--------|------------|------------------------------------------------|
| V7     | 2025-XX-XX | Configuração inicial (com erros)               |
| V8     | 2025-XX-XX | Correção manual de erros V7                    |
| V9     | 2026-01-XX | Consolidação automatizada (pipeline funcional) |

---

**📌 Nota Final:**

Este documento é uma **referência viva** e deve ser atualizado conforme o pipeline evolui. Para questões técnicas detalhadas, consulte o código-fonte em [`src/pipeline/consolidator.py`](file:///c:/Users/vinicios.buzzi/buzzi/geovalida/src/pipeline/consolidator.py).
