# Unidade Territorial de Planejamento (UTP) 

**Ferramenta de Suporte a Decisão para Validação e Regionalização de Unidades de Planejamento Territorial (UTPs)**

Desenvolvida para o **LabTrans (UFSC)**, o software automatiza a revisao da malha de UTPs no Brasil, garantindo que nenhum município fique isolado sem justificativa técnica, utilizando fluxos de transporte e hierarquia urbana do IBGE.

---

## Sumário

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

## Visão Geral

O software processa dados territoriais brasileiros seguindo uma hierarquia de três níveis:

```
BRASIL
  ├── Região Metropolitana (RM)
  │    ├── UTP (Unidade de Planejamento Territorial)
  │    │    ├── Municipio
  │    │    ├── Municipio
  │    │    └── ...
```

O sistema utiliza algoritmos de grafos (NetworkX) e analise espacial (GeoPandas) para consolidar municipios em regioes funcionais coerentes.

---

## Objetivo Central

Automatizar a revisão da malha de UTPs, resolvendo três problemas principais:

1. **Municípios Isolados (Unitários)**: Municípios que formam uma UTP sozinhos sem justificativa.
2. **Falta de Contiguidade**: Municípios que não conseguem chegar à sede da UTP por estar desconectados geograficamente.
3. **Inconsistência Funcional**: Falta de fluxo de transporte que justifique a permanência em uma UTP específica.

---

## Regras de Negócio

### Hierarquia de Consolidação

O projeto opera sob uma hierarquia de três níveis: **Região Metropolitana (RM) -> UTP -> Município**.

### Metodologia de Consolidação (Versões 8.x)

O processo de consolidação evoluiu para uma série de refinamentos sucessivos:

- **Versão 8.0 - Distribuição Inicial**: Configuração base da malha de UTPs.
- **Versão 8.1 - UTPs Unitárias**: Tratamento de municípios isolados via fluxos funcionais ou adjacência.
- **Versão 8.2 - Dependência entre Sedes**: Análise de fluxos entre sedes de UTPs para consolidação de centros regionais.
- **Versão 8.3 - Centralização das Sedes**: Refinamento final baseado na polarização e alcance das sedes consolidadas.

### Critérios de Decisão

#### 1. Hierarquia REGIC
O município é movido para a UTP vizinha que possua a sede com maior influência urbana (Metrópole Nacional > Metrópole > Capital Regional A, etc.).

#### 2. Fluxos Funcionais
Utiliza a Matriz Origem-Destino para identificar dependências socioeconômicas.
Critério: `Fluxo_Total >= Threshold_Minimo`.

#### 3. Proximidade e Fronteira
Em casos de empate técnico, utiliza-se a menor distância entre sedes ou a maior extensão de fronteira partilhada (calculada em metros usando EPSG:5880).

---

## Requisitos

### Sistema Operacional
- Windows 10+, macOS 10.14+, Linux (Ubuntu 18.04+)

### Python
- Python 3.10+ (recomendado 3.12+)

### Dados Necessários
Os arquivos de dados devem ser colocados em `data/01_raw/`:
- `UTP_FINAL.csv`: Base de UTPs por município.
- `SEDE+regic.csv`: Sedes e niveis REGIC.
- `person-matrix-data/`: Matrizes de fluxo (aeroviária, rodoviária, etc.).
- `impedance/`: Dados de impedância e tempos de viagem.
- `shapefiles/`: Arquivos de malha municipal do IBGE.

---

## Instalação

### Passo 1: Preparar o Repositório

```bash
git clone <url-do-repositorio>
cd geovalida
```

### Passo 2: Criar e Ativar Ambiente Virtual

```bash
python -m venv venv
```

**Windows**: `.\venv\Scripts\activate`
**macOS/Linux**: `source venv/bin/activate`

### Passo 3: Instalar Dependências

```bash
pip install -r requirements.txt
```

---

## Como Usar

### Opção 1: Dashboard Interativo (Streamlit)

```bash
streamlit run app.py
```

A interface permite visualizar mapas, analisar fluxos e executar os passos de consolidação de forma interativa.

### Opção 2: Pipeline via CLI

```bash
python main.py
```

Executa o pipeline completo, desde o carregamento de dados até a exportação dos resultados consolidados.

---

## Estrutura do Projeto

```
geovalida/
├── app.py                      # Entrada do dashboard Streamlit
├── main.py                     # Entrada do pipeline CLI
├── src/
│   ├── core/                   # Logica central do sistema
│   │   ├── graph.py            # Gerenciamento da hierarquia (NetworkX)
│   │   ├── manager.py          # Coordenacao de processos
│   │   └── validator.py        # Validacao de regras territoriais
│   ├── pipeline/               # Passos do processamento de dados
│   │   ├── analyzer.py         # Analise de fluxos OD
│   │   ├── consolidator.py     # Logica de fusao de UTPs
│   │   ├── sede_analyzer.py    # Analise detalhada de sedes
│   │   └── sede_consolidator.py # Regras de consolidacao entre sedes
│   └── interface/              # Componentes da interface visual
│       ├── dashboard.py        # Renderizacao principal
│       ├── flow_utils.py       # Utilitarios para visualizacao de fluxos
│       └── snapshot_loader.py  # Carregamento de estados intermediarios
├── data/
│   ├── 01_raw/                 # Dados brutos de entrada
│   ├── 02_intermediate/        # Resultados de etapas intermediarias
│   └── 04_maps/                # Mapas gerados em GeoJSON/PNG
└── tests/                      # Testes unitarios e de integracao
```

---

## Arquitetura

### Componentes Core

#### Hierarquia Territorial (graph.py)
Utiliza grafos direcionados para representar a estrutura RM -> UTP -> Município, facilitando operações de realocação e busca de UTPs unitárias.

#### Validação Territorial (validator.py)
Aplica análise espacial via GeoPandas para verificar contiguidade geográfica e calcular indicadores de vizinhança.

#### Análise de Fluxos (analyzer.py / sede_analyzer.py)
Quantifica a interação entre municípios e sedes, fundamentando a consolidação funcional além da simples adjacência física.

---

## Troubleshooting

- **Erro de Importação (GeoPandas/GDAL)**: Certifique-se de que o ambiente virtual está ativo e as dependências foram instaladas corretamente. No Windows, recomenda-se o uso de wheels do site da documentação oficial se houver falhas no pip.
- **Arquivos não encontrados**: Verifique se a estrutura em `data/01_raw/` segue exatamente o padrão descrito na seção de Requisitos.
- **Performance**: O processamento de grandes malhas (5500+ municípios) consome memória significativa. Recomenda-se no mínimo 8GB de RAM.

---

**Laboratório de Transportes e Logística (LabTrans) - UFSC**
Website: [labtrans.ufsc.br](https://labtrans.ufsc.br)
Última atualização: Fevereiro 2026
