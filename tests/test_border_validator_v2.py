# tests/test_border_validator_v2.py
"""
Testes para o módulo BorderValidatorV2.

Casos de teste conhecidos:
1. Valença (BA) - código 2932903
   - Deve ter como principal fluxo_sede a sede de Cairu
   - Distância: até 2 horas
   
2. Floresta (PE) - código 2605707
   - Deve ter como principal fluxo_sede a sede de Serra Talhada
   - Distância: até 2 horas
"""

import pytest
import pandas as pd
import geopandas as gpd
from pathlib import Path
import logging

from src.core.graph import TerritorialGraph
from src.core.validator import TerritorialValidator
from src.pipeline.border_validator_v2 import BorderValidatorV2


# Configurar logging para visualizar os detalhes durante os testes
logging.basicConfig(level=logging.DEBUG)


class TestBorderValidatorV2:
    """Testes para o BorderValidatorV2"""
    
    @pytest.fixture(scope="class")
    def data_paths(self):
        """Retorna os caminhos dos arquivos de dados necessários."""
        base_path = Path(__file__).parent.parent
        return {
            'initialization': base_path / "data" / "initialization.json",
            'impedance': base_path / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv",
            'flow_data': base_path / "data" / "01_raw" / "fluxo_turistico_agregado.csv",
            'shapefile': base_path / "data" / "01_raw" / "BR_Municipios_2022.shp"
        }
    
    @pytest.fixture(scope="class")
    def graph_and_validator(self, data_paths):
        """Inicializa o grafo territorial e o validador."""
        # Criar grafo
        graph = TerritorialGraph()
        
        # Carregar dados de inicialização se existirem
        if data_paths['initialization'].exists():
            import json
            with open(data_paths['initialization'], 'r', encoding='utf-8') as f:
                init_data = json.load(f)
            
            # Configurar grafo com dados de inicialização
            # (adapte conforme sua estrutura de dados)
            if 'municipios' in init_data:
                for mun in init_data['municipios']:
                    # Try both cases to be safe or inspect keys
                    if 'cd_mun' in mun:
                        mun_id = int(mun['cd_mun'])
                    elif 'CD_MUN' in mun:
                        mun_id = int(mun['CD_MUN'])
                    else:
                        continue
                        
                    graph.hierarchy.add_node(
                        mun_id,
                        type='municipality',
                        name=mun.get('nm_mun', mun.get('NM_MUN', '')),
                        regiao_metropolitana=mun.get('regiao_metropolitana', '')
                    )
        
        # Criar validador
        validator = TerritorialValidator(graph)
        
        return graph, validator
    
    @pytest.fixture(scope="class")
    def flow_data(self, data_paths):
        """Carrega dados de fluxo turístico e combina com impedância."""
        # Caminhos dos arquivos
        flow_path = data_paths['initialization'].parent / "01_raw" / "person-matrix-data" / "base_dados_rodoviaria_particular_2023.csv"
        impedance_path = data_paths['impedance']
        
        if not flow_path.exists() or not impedance_path.exists():
            return None
            
        # Carregar fluxo
        try:
            df_flow = pd.read_csv(flow_path)
            # Garantir tipos compatíveis
            df_flow['mun_origem'] = df_flow['mun_origem'].astype(int)
            df_flow['mun_destino'] = df_flow['mun_destino'].astype(int)
        except Exception as e:
            logging.error(f"Erro ao carregar fluxo: {e}")
            return None
            
        # Carregar impedância
        try:
            # Impedância usa ponto e vírgula e vírgula decimal
            df_impedance = pd.read_csv(impedance_path, sep=';', decimal=',')
            
            # Renomear colunas para facilitar merge
            df_impedance = df_impedance.rename(columns={
                'COD_IBGE_ORIGEM': 'mun_origem',
                'COD_IBGE_DESTINO': 'mun_destino',
                'Tempo': 'tempo_viagem'
            })
            
            # Converter tipos e tratar erros
            df_impedance['mun_origem'] = pd.to_numeric(df_impedance['mun_origem'], errors='coerce').fillna(0).astype(int)
            df_impedance['mun_destino'] = pd.to_numeric(df_impedance['mun_destino'], errors='coerce').fillna(0).astype(int)
            df_impedance['tempo_viagem'] = pd.to_numeric(df_impedance['tempo_viagem'], errors='coerce').fillna(999.0)
            
        except Exception as e:
            logging.error(f"Erro ao carregar impedância: {e}")
            return df_flow  # Retornar só fluxo se impedância falhar (teste vai falhar na checagem de tempo)
            
        # Merge (inner join para ter ambos)
        # Nota: O BorderValidatorV2 espera as colunas: mun_origem, mun_destino, viagens, tempo_viagem
        df_merged = pd.merge(
            df_flow,
            df_impedance[['mun_origem', 'mun_destino', 'tempo_viagem']],
            on=['mun_origem', 'mun_destino'],
            how='inner'
        )
        
        return df_merged
    
    @pytest.fixture(scope="class")
    def geodata(self, data_paths):
        """Carrega dados geoespaciais."""
        # Caminho correto do shapefile
        shp_path = data_paths['initialization'].parent / "01_raw" / "shapefiles" / "BR_Municipios_2024.shp"
        if shp_path.exists():
            gdf = gpd.read_file(shp_path)
            return gdf
        return None
    
    @pytest.fixture(scope="class")
    def border_validator(self, graph_and_validator, data_paths):
        """Cria uma instância do BorderValidatorV2."""
        graph, validator = graph_and_validator
        return BorderValidatorV2(graph, validator, data_dir=data_paths['initialization'].parent / "03_processed")
    
    def test_initialization(self, border_validator):
        """Testa a inicialização do BorderValidatorV2."""
        assert border_validator is not None
        assert border_validator.graph is not None
        assert border_validator.validator is not None
        assert border_validator.logger is not None
    
    def test_valenca_ba_flow_to_cairu(self, border_validator, flow_data):
        """
        Testa se Valença (BA) tem fluxo para Cairu dentro de 2 horas.
        
        Valença (BA): 2932903
        Esperado: Tem fluxo significativo para a sede de Cairu
        """
        if flow_data is None:
            pytest.skip("Dados de fluxo não disponíveis")
        
        valenca_id = 2932903
        
        # Verificar se há dados de fluxo para Valença
        flows_from_valenca = flow_data[
            flow_data['mun_origem'].astype(int) == valenca_id
        ]
        
        assert not flows_from_valenca.empty, f"Nenhum fluxo encontrado partindo de Valença (BA) {valenca_id}"
        
        # Obter fluxos para sedes dentro de 2h
        sede_flows = border_validator._get_flows_to_sedes(valenca_id, flow_data, max_time=2.0)
        
        assert len(sede_flows) > 0, f"Valença (BA) {valenca_id} deveria ter fluxo para pelo menos uma sede dentro de 2h"
        
        # Logar os principais fluxos para análise
        print(f"\n=== Fluxos de Valença (BA) para sedes (≤2h) ===")
        for sede_id, flow_value, travel_time in sede_flows:
            sede_utp = border_validator.graph.get_municipality_utp(sede_id)
            sede_name = border_validator.graph.hierarchy.nodes.get(sede_id, {}).get('name', str(sede_id))
            print(f"  → {sede_name} (ID: {sede_id}, UTP: {sede_utp}): {flow_value:.0f} viagens, {travel_time:.2f}h")
        
        # Verificar se Cairu está entre os destinos (se soubermos o ID de Cairu)
        # Nota: Você precisará descobrir o código IBGE de Cairu para fazer esta verificação
        # Por enquanto, apenas verificamos que há fluxos para sedes
    
    def test_floresta_pe_flow_to_serra_talhada(self, border_validator, flow_data):
        """
        Testa se Floresta (PE) tem fluxo para Serra Talhada dentro de 2 horas.
        
        Floresta (PE): 2605707
        Esperado: Tem fluxo significativo para a sede de Serra Talhada
        """
        if flow_data is None:
            pytest.skip("Dados de fluxo não disponíveis")
        
        floresta_id = 2605707
        
        # Verificar se há dados de fluxo para Floresta
        flows_from_floresta = flow_data[
            flow_data['mun_origem'].astype(int) == floresta_id
        ]
        
        assert not flows_from_floresta.empty, f"Nenhum fluxo encontrado partindo de Floresta (PE) {floresta_id}"
        
        # Obter fluxos para sedes dentro de 2h
        sede_flows = border_validator._get_flows_to_sedes(floresta_id, flow_data, max_time=2.0)
        
        assert len(sede_flows) > 0, f"Floresta (PE) {floresta_id} deveria ter fluxo para pelo menos uma sede dentro de 2h"
        
        # Logar os principais fluxos para análise
        print(f"\n=== Fluxos de Floresta (PE) para sedes (≤2h) ===")
        for sede_id, flow_value, travel_time in sede_flows:
            sede_utp = border_validator.graph.get_municipality_utp(sede_id)
            sede_name = border_validator.graph.hierarchy.nodes.get(sede_id, {}).get('name', str(sede_id))
            print(f"  → {sede_name} (ID: {sede_id}, UTP: {sede_utp}): {flow_value:.0f} viagens, {travel_time:.2f}h")
        
        # Serra Talhada - precisamos descobrir o código IBGE
        # Por enquanto, verificamos que há fluxos
    
    def test_build_adjacency_graph(self, border_validator, geodata):
        """Testa a construção do grafo de adjacências."""
        if geodata is None:
            pytest.skip("Dados geoespaciais não disponíveis")
        
        border_validator._build_adjacency_graph(geodata)
        
        assert border_validator.adjacency_graph is not None
        assert border_validator.adjacency_graph.number_of_nodes() > 0
        assert border_validator.adjacency_graph.number_of_edges() > 0
        
        print(f"\n=== Grafo de Adjacências ===")
        print(f"Nós: {border_validator.adjacency_graph.number_of_nodes()}")
        print(f"Arestas: {border_validator.adjacency_graph.number_of_edges()}")
    
    def test_valenca_is_adjacent_to_target_utp(self, border_validator, geodata):
        """
        Testa se Valença está adjacente a alguma UTP vizinha.
        """
        if geodata is None:
            pytest.skip("Dados geoespaciais não disponíveis")
        
        valenca_id = 2932903
        
        # Construir grafo de adjacências se ainda não foi construído
        if border_validator.adjacency_graph is None:
            border_validator._build_adjacency_graph(geodata)
        
        # Verificar se Valença está no grafo
        if valenca_id not in border_validator.adjacency_graph:
            pytest.skip(f"Valença {valenca_id} não encontrada no grafo de adjacências")
        
        # Obter vizinhos
        neighbors = list(border_validator.adjacency_graph[valenca_id])
        
        assert len(neighbors) > 0, f"Valença {valenca_id} deveria ter municípios adjacentes"
        
        print(f"\n=== Municípios Adjacentes a Valença (BA) ===")
        for neighbor in neighbors[:10]:  # Mostrar apenas os primeiros 10
            neighbor_name = border_validator.graph.hierarchy.nodes.get(neighbor, {}).get('name', str(neighbor))
            neighbor_utp = border_validator.graph.get_municipality_utp(neighbor)
            print(f"  → {neighbor_name} (ID: {neighbor}, UTP: {neighbor_utp})")
    
    def test_floresta_is_adjacent_to_target_utp(self, border_validator, geodata):
        """
        Testa se Floresta (PE) está adjacente a alguma UTP vizinha.
        """
        if geodata is None:
            pytest.skip("Dados geoespaciais não disponíveis")
        
        floresta_id = 2605707
        
        # Construir grafo de adjacências se ainda não foi construído
        if border_validator.adjacency_graph is None:
            border_validator._build_adjacency_graph(geodata)
        
        # Verificar se Floresta está no grafo
        if floresta_id not in border_validator.adjacency_graph:
            pytest.skip(f"Floresta {floresta_id} não encontrada no grafo de adjacências")
        
        # Obter vizinhos
        neighbors = list(border_validator.adjacency_graph[floresta_id])
        
        assert len(neighbors) > 0, f"Floresta {floresta_id} deveria ter municípios adjacentes"
        
        print(f"\n=== Municípios Adjacentes a Floresta (PE) ===")
        for neighbor in neighbors[:10]:  # Mostrar apenas os primeiros 10
            neighbor_name = border_validator.graph.hierarchy.nodes.get(neighbor, {}).get('name', str(neighbor))
            neighbor_utp = border_validator.graph.get_municipality_utp(neighbor)
            print(f"  → {neighbor_name} (ID: {neighbor}, UTP: {neighbor_utp})")
    
    def test_find_better_utp_for_valenca(self, border_validator, flow_data, geodata):
        """
        Testa se o validador consegue identificar uma UTP melhor para Valença.
        """
        if flow_data is None or geodata is None:
            pytest.skip("Dados necessários não disponíveis")
        
        valenca_id = 2932903
        
        # Construir grafo de adjacências
        if border_validator.adjacency_graph is None:
            border_validator._build_adjacency_graph(geodata)
        
        # Obter UTP atual de Valença
        current_utp = border_validator.graph.get_municipality_utp(valenca_id)
        
        if not current_utp:
            pytest.skip(f"Valença {valenca_id} não possui UTP atribuída")
        
        print(f"\n=== Análise de Realocação para Valença (BA) ===")
        print(f"UTP Atual: {current_utp}")
        
        # Procurar UTP melhor
        result = border_validator._find_better_utp(valenca_id, current_utp, flow_data)
        
        if result:
            target_utp, flow_value, reason = result
            print(f"✅ UTP Melhor Encontrada: {target_utp}")
            print(f"   Fluxo: {flow_value:.0f} viagens")
            print(f"   Razão: {reason}")
        else:
            print(f"ℹ️  Nenhuma UTP melhor encontrada (Valença já está bem posicionada)")
    
    def test_find_better_utp_for_floresta(self, border_validator, flow_data, geodata):
        """
        Testa se o validador consegue identificar uma UTP melhor para Floresta.
        """
        if flow_data is None or geodata is None:
            pytest.skip("Dados necessários não disponíveis")
        
        floresta_id = 2605707
        
        # Construir grafo de adjacências
        if border_validator.adjacency_graph is None:
            border_validator._build_adjacency_graph(geodata)
        
        # Obter UTP atual de Floresta
        current_utp = border_validator.graph.get_municipality_utp(floresta_id)
        
        if not current_utp:
            pytest.skip(f"Floresta {floresta_id} não possui UTP atribuída")
        
        print(f"\n=== Análise de Realocação para Floresta (PE) ===")
        print(f"UTP Atual: {current_utp}")
        
        # Procurar UTP melhor
        result = border_validator._find_better_utp(floresta_id, current_utp, flow_data)
        
        if result:
            target_utp, flow_value, reason = result
            print(f"✅ UTP Melhor Encontrada: {target_utp}")
            print(f"   Fluxo: {flow_value:.0f} viagens")
            print(f"   Razão: {reason}")
        else:
            print(f"ℹ️  Nenhuma UTP melhor encontrada (Floresta já está bem posicionada)")


def test_get_municipality_codes():
    """
    Teste auxiliar para descobrir os códigos IBGE das sedes mencionadas.
    """
    base_path = Path(__file__).parent.parent
    mun_file = base_path / "data" / "01_raw" / "v7_base 2(br_municipios_2024).csv"
    
    if not mun_file.exists():
        pytest.skip("Arquivo de municípios não disponível")
    
    df = pd.read_csv(mun_file, sep=';', encoding='latin1')
    
    # Procurar Cairu
    cairu = df[df.iloc[:, 1].str.contains('Cairu', case=False, na=False)]
    if not cairu.empty:
        print("\n=== Cairu ===")
        print(cairu[cairu.columns[:10]].to_string())
    
    # Procurar Serra Talhada
    serra_talhada = df[df.iloc[:, 1].str.contains('Serra Talhada', case=False, na=False)]
    if not serra_talhada.empty:
        print("\n=== Serra Talhada ===")
        print(serra_talhada[serra_talhada.columns[:10]].to_string())


if __name__ == '__main__':
    # Executar testes com verbose
    pytest.main([__file__, '-v', '-s'])
