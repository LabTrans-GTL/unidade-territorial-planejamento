# 🛰️ GeoValida

**Ferramenta de Suporte à Decisão para Validação e Regionalização de Unidades de Planejamento Territorial (UTPs)**

Desenvolvida para o **LabTrans (UFSC)**, o GeoValida automatiza a revisão da malha de UTPs no Brasil, garantindo que nenhum município fique isolado sem justificativa técnica, utilizando fluxos de transporte e hierarquia urbana do IBGE.

---

## 📋 Sumário

- [Visão Geral](#visão-geral)
- [Objetivo Central](#objetivo-central)
- [Regras de Negócio](#regras-de-negócio)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Como Usar](#como-usar)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Arquitetura](#arquitetura)
- [Troubleshooting](#troubleshooting)

---

## 🎯 Visão Geral

O GeoValida processa dados territoriais brasileiros seguindo uma hierarquia de três níveis:

```
BRASIL
  ├── Região Metropolitana (RM)
  │    ├── UTP (Unidade de Planejamento Territorial)
  │    │    ├── Município
  │    │    ├── Município
  │    │    └── ...
```

O sistema utiliza **algoritmos de grafos** (NetworkX) e **análise espacial** (GeoPandas) para consolidar municípios em regiões funcionais coerentes.

---

## 🎯 Objetivo Central

Automatizar a revisão da malha de UTPs, resolvendo três problemas principais:

1. **Municípios Isolados (Unitários)**: Municípios que formam uma UTP sozinhos sem justificativa
2. **Falta de Contiguidade**: Municípios que não conseguem chegar à sede da UTP por estar desconectados
3. **Inconsistência Funcional**: Falta de fluxo de transporte justificando a permanência na UTP

---

## 📊 Regras de Negócio

### Hierarquia de Consolidação

O projeto opera sob uma **hierarquia de três níveis**: **Região Metropolitana (RM) → UTP → Município**

### Passo 5: Consolidação Funcional (Fluxos)

Municípios que **não pertencem a RMs** e estão em **UTPs unitárias** são fundidos a UTPs vizinhas se houver um fluxo significativo de viagens (Matriz OD) que justifique a dependência funcional.

**Critério**: `Fluxo_Total ≥ Threshold_Mínimo`

### Passo 7: Limpeza Territorial (REGIC + Adjacência)

UTPs unitárias remanescentes (sem fluxo claro) são resolvidas via **REGIC + Adjacência**:

#### 1️⃣ Hierarquia REGIC
O município é movido para a UTP vizinha que possua a sede com maior influência urbana:
- Metrópole Nacional > Metrópole > Capital Regional A > ... > Centro Local

#### 2️⃣ Distância
Em caso de empate no REGIC, escolhe-se a sede mais próxima.

#### 3️⃣ Envolvência (Fronteira)
Como último desempate, escolhe-se a UTP com **maior extensão de fronteira partilhada** (medida em metros, usando **EPSG:5880** para precisão métrica).

---

## 📦 Requisitos

### Sistema Operacional
- Windows 10+, macOS 10.14+, Linux (Ubuntu 18.04+)

### Python
- Python 3.10+ (recomendado 3.12+)

### Dependências de Sistema (GeoPandas/GDAL)
- **Windows**: Incluídas automaticamente via wheels pré-compiladas
- **macOS**: `brew install gdal`
- **Linux**: `sudo apt-get install gdal-bin libgdal-dev`

### Dados Necessários
Coloque os seguintes arquivos em `data/01_raw/`:

```
data/01_raw/
├── UTP_FINAL.csv                          # Base de UTPs por município
├── SEDE+regic.csv                         # Sedes e níveis REGIC
├── person-matrix-data/
│   ├── base_dados_aeroviaria_2023.csv
│   ├── base_dados_ferroviaria_2023.csv
│   ├── base_dados_hidroviaria_2023.csv
│   ├── base_dados_rodoviaria_coletiva_2023.csv
│   └── base_dados_rodoviaria_particular_2023.csv
├── impedance/
│   └── impedancias_filtradas_2h.csv
└── shapefiles/
    ├── BR_Municipios_2024.shp
    ├── BR_Municipios_2024.shx
    ├── BR_Municipios_2024.dbf
    ├── BR_Municipios_2024.prj
    └── ... (outros arquivos .cpg, .qmd)
```

---

## 🚀 Instalação

### Passo 1: Clonar/Preparar o Repositório

```powershell
cd C:\Users\vinicios.buzzi\buzzi\GeoValida
```

### Passo 2: Criar Virtual Environment

```powershell
python -m venv venv
```

### Passo 3: Ativar Virtual Environment

**Windows (PowerShell)**:
```powershell
.\venv\Scripts\Activate.ps1
```

**Windows (CMD)**:
```cmd
.\venv\Scripts\activate.bat
```

**macOS/Linux**:
```bash
source venv/bin/activate
```

### Passo 4: Instalar Dependências

```powershell
pip install -r requirements.txt
```

### Passo 5: Verificar Instalação

```powershell
python -c "import pandas, geopandas, networkx, matplotlib; print('✓ Tudo OK')"
```

---

## 📖 Como Usar

### Opção 1: Dashboard Interativo (Streamlit) ⭐ Recomendado

```powershell
streamlit run app.py
```

Isso abre a interface web em `http://localhost:8501` com:
- Carregamento de dados step-by-step
- Visualização de fluxos
- Geração de mapas interativos
- Consolidação de UTPs

### Opção 2: Pipeline CLI (Terminal)

```powershell
python main.py
```

Executa o pipeline completo automaticamente:
1. Carrega dados
2. Gera mapa inicial
3. Analisa fluxos OD
4. Consolida UTPs (Passo 5)
5. Aplica REGIC + Adjacência (Passo 7)
6. Exporta resultado final

### Opção 3: Teste Rápido

Para verificar se o sistema está funcionando sem carregar dados completos:

```powershell
python -c "from main import GeoValidaManager; print('✓ Sistema pronto')"
```

---

## 📁 Estrutura do Projeto

```
GeoValida/
├── app.py                          # Entrada Streamlit (interface visual)
├── main.py                         # Entrada CLI (pipeline automático)
├── requirements.txt                # Dependências Python
├── README.md                       # Este arquivo
│
├── src/
│   ├── __init__.py
│   ├── config.py                   # Configurações (caminhos, logging)
│   │
│   ├── core/                       # Lógica principal
│   │   ├── __init__.py
│   │   ├── graph.py                # Gerenciamento de hierarquia (NetworkX)
│   │   └── validator.py            # Validação territorial (GeoPandas)
│   │
│   ├── pipeline/                   # Processamento de dados
│   │   ├── __init__.py
│   │   ├── analyzer.py             # Análise de Matriz OD
│   │   ├── consolidator.py         # Consolidação de UTPs
│   │   └── mapper.py               # Geração de mapas
│   │
│   └── interface/                  # Interface visual
│       ├── __init__.py
│       ├── dashboard.py            # Renderização principal
│       └── components/
│           ├── __init__.py
│           ├── sidebar.py          # Barra lateral
│           ├── metrics.py          # Métricas do topo
│           └── map.viewer.py       # Visualizador de mapas
│
├── data/
│   ├── 01_raw/                     # Dados brutos (CSVs, shapefiles)
│   ├── 02_intermediate/            # Dados processados intermediários
│   ├── 03_final/                   # Resultado final
│   └── 04_maps/                    # Mapas gerados
│
└── venv/                           # Virtual environment
```

---

## 🏗️ Arquitetura

### Componentes Core

#### `graph.py` - Hierarquia Territorial
Gerencia a estrutura hierárquica usando **NetworkX**:
- Nó raiz: BRASIL
- Nível 1: Regiões Metropolitanas (RM)
- Nível 2: Unidades de Planejamento Territorial (UTP)
- Nível 3: Municípios

**Principais métodos**:
- `add_municipality()` - Adiciona município ao grafo
- `move_municipality()` - Transfere município entre UTPs
- `get_unitary_utps()` - Lista UTPs com apenas 1 município
- `load_from_dataframe()` - Popula grafo a partir de CSV

#### `validator.py` - Validação Territorial
Implementa regras de negócio usando **GeoPandas**:
- Cálculo de REGIC score
- Detecção de contiguidade
- Medição de fronteiras (EPSG:5880)
- Adjacência geográfica

**Principais métodos**:
- `get_regic_score()` - Score de influência urbana
- `get_shared_boundary_length()` - Comprimento de fronteira em metros
- `validate_utp_contiguity()` - Busca municípios isolados
- `is_adjacent_to_any_in_utp()` - Verifica adjacência

#### `analyzer.py` - Análise de Fluxos
Processa Matriz Origem-Destino:
- `run_full_analysis()` - Carrega CSVs de person-matrix-data
- `get_main_destination()` - Encontra destino principal
- `filter_significant_flows()` - Filtra fluxos acima do threshold

#### `consolidator.py` - Consolidação
Implementa os passos 5 e 7:
- `run_functional_merging()` - Consolida por fluxos (Passo 5)
- `run_territorial_regic()` - Consolida por REGIC+adjacência (Passo 7)

#### `mapper.py` - Geração de Mapas
Sincroniza grafo com geometrias e gera visualizações:
- `load_shapefiles()` - Carrega BR_Municipios_2024.shp
- `sync_with_graph()` - Atualiza UTP_ID conforme grafo
- `save_map()` - Exporta PNG com cores por UTP

---

## 🛠️ Troubleshooting

### ❌ "ImportError: No module named 'geopandas'"

**Solução**:
```powershell
pip install geopandas
```

### ❌ "FileNotFoundError: data/01_raw/UTP_FINAL.csv not found"

**Solução**: Coloque os arquivos CSV/shapefiles em `data/01_raw/` conforme listado em [Requisitos](#requisitos).

### ❌ "Import streamlit could not be resolved"

**Solução**: Streamlit não estava no requirements.txt original. Já foi adicionado. Reinstale:
```powershell
pip install streamlit
```

### ❌ "GDAL error" no load_shapefiles()

**Solução**: Problema ao ler shapefile. Verifique se:
- Arquivo `.shp` existe
- Arquivo `.shx` (índice) existe
- Arquivo `.dbf` (dados) existe
- Arquivo `.prj` (CRS) existe

### ❌ Streamlit abre mas não carrega dados

**Solução**: Verifique os logs no terminal. Se os CSVs têm encoding incorreto, edite o `encoding='latin1'` em `main.py` linha 39.

### ❌ Performance lenta com 5500+ municípios

**Solução**: O `validate_utp_contiguity()` usa `sjoin()` otimizado. Se ainda for lento:
1. Reduza `max_results` em buscas
2. Use `gdf.spatial_index` para indexação
3. Processe por região (RM) separadamente

---

## 📊 Exemplo de Uso Prático

### Cenário: Consolidar UTPs unitárias

1. **Inicie o Streamlit**:
   ```powershell
   streamlit run app.py
   ```

2. **Carregue os dados** (aba "⚙️ Processamento"):
   - Clique em "Carregar Dados"
   - Aguarde carregamento dos CSVs e shapefiles

3. **Visualize a situação inicial** (aba "🗺️ Visualização"):
   - Mapa mostra UTPs atuais
   - Identifica UTPs unitárias em vermelho

4. **Execute o Passo 5** (Consolidação por Fluxos):
   - Municípios isolados sem fluxo claro são fundidos
   - Relatório mostra quantos foram consolidados

5. **Execute o Passo 7** (REGIC + Adjacência):
   - Remanescentes são resolvidos por hierarquia REGIC
   - Desempates por distância/fronteira

6. **Exporte o resultado**:
   - Mapa final é gerado em `data/04_maps/FINAL_CONSOLIDADO.png`
   - CSV é exportado para `data/02_intermediate/`

---

## 📝 Parâmetros Importantes

Edite em `src/config.py`:

```python
FILES = {
    "utp_base": RAW_DIR / "UTP_FINAL.csv",
    "sede_regic": RAW_DIR / "SEDE+regic.csv",
    "matriz_pessoas": RAW_DIR / "person-matrix-data",
    "impedancias": RAW_DIR / "impedance" / "impedancias_filtradas_2h.csv",
    "shapefiles": RAW_DIR / "shapefiles",
}
```

Edite em `src/pipeline/analyzer.py`:

```python
FLOW_THRESHOLD = 0.05  # Proporção mínima de fluxo para consolidação
```

Edite em `src/core/validator.py`:

```python
BUFFER_DEGREES = 0.01  # ~1km para detecção de adjacência
CRS_METRIC = "EPSG:5880"  # Projeção brasileira para medições em metros
```

---

## 🤝 Contribuindo

Para melhorias ou correções, crie um issue ou pull request.

---

## 📧 Contato

**LabTrans - UFSC**
- Email: [vinicios.labtrans@gmail.com]
- Website: [labrans.ufsc.br]
- app: [geovalida.streamlit.app]

---

## 📄 Licença

Este projeto é desenvolvido pelo LabTrans (UFSC) para fins de pesquisa.

---

## 🎓 Referências

- **NetworkX**: [https://networkx.org/](https://networkx.org/)
- **GeoPandas**: [https://geopandas.org/](https://geopandas.org/)
- **Shapely**: [https://shapely.readthedocs.io/](https://shapely.readthedocs.io/)
- **Streamlit**: [https://streamlit.io/](https://streamlit.io/)
- **IBGE REGIC**: [https://www.ibge.gov.br/](https://www.ibge.gov.br/)

---

**Última atualização**: Janeiro 2026 | **Versão**: 1.0.0
