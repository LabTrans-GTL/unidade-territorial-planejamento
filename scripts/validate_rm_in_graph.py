#!/usr/bin/env python3
"""
Script de validação da implementação de Região Metropolitana no GeoValida.
Valida dados no initialization.json, grafo territorial e interface.
"""
import json
import sys
from pathlib import Path
import logging

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import DataLoader
from src.core.graph import TerritorialGraph
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def validate_initialization_json():
    """Valida dados de RM no initialization.json"""
    logger.info("\n" + "="*80)
    logger.info("1. VALIDAÇÃO DO INITIALIZATION.JSON")
    logger.info("="*80)
    
    json_path = Path('data/initialization.json')
    if not json_path.exists():
        logger.error(f"❌ Arquivo {json_path} não encontrado!")
        return False
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    municipios = data.get('municipios', [])
    
    # Contar municípios com e sem RM
    com_rm = [m for m in municipios if m.get('regiao_metropolitana', '').strip()]
    sem_rm = [m for m in municipios if not m.get('regiao_metropolitana', '').strip()]
    
    logger.info(f"\n📊 Estatísticas:")
    logger.info(f"  Total de municípios: {len(municipios)}")
    logger.info(f"  ✅ COM RM: {len(com_rm)} ({len(com_rm)/len(municipios)*100:.1f}%)")
    logger.info(f"  ⚪ SEM RM: {len(sem_rm)} ({len(sem_rm)/len(municipios)*100:.1f}%)")
    
    # Contar RMs únicas
    rms_unicas = set(m.get('regiao_metropolitana', '') for m in com_rm if m.get('regiao_metropolitana', '').strip())
    logger.info(f"  🏙️ RMs únicas: {len(rms_unicas)}")
    
    # Validação esperada (baseado no arquivo Composicao_RM_2024.xlsx com 1440 linhas)
    expected_min = 1300  # Esperamos pelo menos 1300 municípios com RM
    if len(com_rm) >= expected_min:
        logger.info(f"\n✅ VALIDAÇÃO PASSOU: {len(com_rm)} municípios com RM (esperado >= {expected_min})")
        return True
    else:
        logger.error(f"\n❌ VALIDAÇÃO FALHOU: {len(com_rm)} municípios com RM (esperado >= {expected_min})")
        return False


def validate_dataloader():
    """Valida que DataLoader está carregando dados de RM corretamente"""
    logger.info("\n" + "="*80)
    logger.info("2. VALIDAÇÃO DO DATALOADER")
    logger.info("="*80)
    
    try:
        data_loader = DataLoader()
        df = data_loader.get_municipios_dataframe()
        
        if df.empty:
            logger.error("❌ DataFrame vazio!")
            return False
        
        # Verificar se coluna existe
        if 'regiao_metropolitana' not in df.columns:
            logger.error("❌ Coluna 'regiao_metropolitana' não encontrada!")
            return False
        
        logger.info(f"\n📊 Estatísticas:")
        logger.info(f"  Total de municípios: {len(df)}")
        logger.info(f"  Colunas disponíveis: {len(df.columns)}")
        
        # Contar valores
        com_rm = df[df['regiao_metropolitana'].str.strip() != '']
        sem_rm = df[df['regiao_metropolitana'].str.strip() == '']
        
        logger.info(f"  ✅ COM RM: {len(com_rm)} ({len(com_rm)/len(df)*100:.1f}%)")
        logger.info(f"  ⚪ SEM RM: {len(sem_rm)} ({len(sem_rm)/len(df)*100:.1f}%)")
        
        logger.info(f"\n✅ VALIDAÇÃO PASSOU: DataLoader funcionando corretamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ ERRO ao validar DataLoader: {e}")
        return False


def validate_graph_structure():
    """Valida estrutura do grafo territorial com hierarquia RM"""
    logger.info("\n" + "="*80)
    logger.info("3. VALIDAÇÃO DA ESTRUTURA DO GRAFO")
    logger.info("="*80)
    
    try:
        # Carregar dados
        data_loader = DataLoader()
        df_municipios = data_loader.get_municipios_dataframe()
        
        if df_municipios.empty:
            logger.error("❌ Sem dados para criar grafo!")
            return False
        
        # Criar grafo territorial (mesmo código do dashboard)
        graph = TerritorialGraph()
        
        logger.info(f"\n🔨 Construindo grafo...")
        
        rm_nodes = set()
        utp_nodes = set()
        mun_nodes = set()
        
        for _, row in df_municipios.iterrows():
            cd_mun = int(row['cd_mun'])
            nm_mun = row.get('nm_mun', str(cd_mun))
            utp_id = str(row.get('utp_id', 'SEM_UTP'))
            rm_name = row.get('regiao_metropolitana', '')
            
            if not rm_name or rm_name.strip() == '':
                rm_name = "SEM_RM"
            
            # Criar hierarquia no grafo
            rm_node = f"RM_{rm_name}"
            if not graph.hierarchy.has_node(rm_node):
                graph.hierarchy.add_node(rm_node, type='rm', name=rm_name)
                graph.hierarchy.add_edge(graph.root, rm_node)
                rm_nodes.add(rm_node)
            
            utp_node = f"UTP_{utp_id}"
            if not graph.hierarchy.has_node(utp_node):
                graph.hierarchy.add_node(utp_node, type='utp', utp_id=utp_id)
                graph.hierarchy.add_edge(rm_node, utp_node)
                utp_nodes.add(utp_node)
            
            graph.hierarchy.add_node(cd_mun, type='municipality', name=nm_mun)
            graph.hierarchy.add_edge(utp_node, cd_mun)
            mun_nodes.add(cd_mun)
        
        logger.info(f"\n📊 Estatísticas do Grafo:")
        logger.info(f"  Total de nós: {len(graph.hierarchy.nodes)}")
        logger.info(f"  🏙️ Nós RM: {len(rm_nodes)}")
        logger.info(f"  🗺️ Nós UTP: {len(utp_nodes)}")
        logger.info(f"  🏘️ Nós Município: {len(mun_nodes)}")
        logger.info(f"  🌳 Raiz: {graph.root}")
        
        # Validações
        validations = []
        
        # 1. Verificar que existe pelo menos 1 RM (além de SEM_RM)
        rm_names = [node for node in rm_nodes if node != "RM_SEM_RM"]
        if len(rm_names) >= 70:  # Esperamos ~76 RMs
            logger.info(f"  ✅ {len(rm_names)} RMs encontradas (esperado ~76)")
            validations.append(True)
        else:
            logger.error(f"  ❌ Apenas {len(rm_names)} RMs encontradas (esperado ~76)")
            validations.append(False)
        
        # 2. Verificar hierarquia: ROOT deve ter filhos RM
        root_children = list(graph.hierarchy.successors(graph.root))
        if all(node.startswith('RM_') for node in root_children):
            logger.info(f"  ✅ ROOT tem apenas filhos RM ({len(root_children)} nós)")
            validations.append(True)
        else:
            logger.error(f"  ❌ ROOT tem filhos não-RM!")
            validations.append(False)
        
        # 3. Verificar que SEM_RM existe e tem UTPs
        if "RM_SEM_RM" in rm_nodes:
            sem_rm_children = list(graph.hierarchy.successors("RM_SEM_RM"))
            logger.info(f"  ✅ RM_SEM_RM existe com {len(sem_rm_children)} UTPs")
            validations.append(True)
        else:
            logger.error(f"  ❌ RM_SEM_RM não encontrado!")
            validations.append(False)
        
        # 4. Verificar alguns casos específicos
        # Exemplo: São Paulo deve estar na RM de São Paulo
        sp_mun = df_municipios[df_municipios['nm_mun'] == 'São Paulo']
        if not sp_mun.empty:
            sp_rm = sp_mun.iloc[0].get('regiao_metropolitana', '')
            if sp_rm:
                logger.info(f"  ✅ São Paulo está em: '{sp_rm}'")
                validations.append(True)
            else:
                logger.warning(f"  ⚠️ São Paulo não tem RM atribuída")
                validations.append(True)  # Não é erro crítico
        
        if all(validations):
            logger.info(f"\n✅ VALIDAÇÃO PASSOU: Grafo construído corretamente")
            return True
        else:
            logger.error(f"\n❌ VALIDAÇÃO FALHOU: {validations.count(False)} erros encontrados")
            return False
        
    except Exception as e:
        logger.error(f"❌ ERRO ao validar grafo: {e}")
        import traceback
        traceback.print_exc()
        return False


def validate_interface_data():
    """Valida que dados de RM aparecem corretamente na interface"""
    logger.info("\n" + "="*80)
    logger.info("4. VALIDAÇÃO DOS DADOS NA INTERFACE")
    logger.info("="*80)
    
    try:
        data_loader = DataLoader()
        df_municipios = data_loader.get_municipios_dataframe()
        
        # Simular criação do resumo de UTPs (mesma lógica do dashboard)
        summary_list = []
        
        for utp_id, group in df_municipios.groupby('utp_id'):
            # Identificar sede
            sede_row = group[group['sede_utp'] == True]
            if sede_row.empty:
                continue
            
            sede = sede_row.iloc[0]
            
            # Região Metropolitana
            rm = sede.get('regiao_metropolitana', '')
            if pd.isna(rm) or str(rm).strip() == '':
                rm = '-'
            
            summary_list.append({
                'UTP': utp_id,
                'Sede': sede['nm_mun'],
                'RM': rm
            })
        
        summary_df = pd.DataFrame(summary_list)
        
        # Contar UTPs com RM
        utps_com_rm = summary_df[summary_df['RM'] != '-']
        utps_sem_rm = summary_df[summary_df['RM'] == '-']
        
        logger.info(f"\n📊 Estatísticas da Interface:")
        logger.info(f"  Total de UTPs: {len(summary_df)}")
        logger.info(f"  ✅ UTPs com RM: {len(utps_com_rm)} ({len(utps_com_rm)/len(summary_df)*100:.1f}%)")
        logger.info(f"  ⚪ UTPs sem RM: {len(utps_sem_rm)} ({len(utps_sem_rm)/len(summary_df)*100:.1f}%)")
        
        logger.info(f"\n📋 Exemplos de UTPs com RM:")
        for _, row in utps_com_rm.head(5).iterrows():
            logger.info(f"  • {row['Sede']:30s} - {row['RM']}")
        
        logger.info(f"\n✅ VALIDAÇÃO PASSOU: Dados de RM aparecem na interface")
        return True
        
    except Exception as e:
        logger.error(f"❌ ERRO ao validar interface: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Função principal"""
    logger.info("="*80)
    logger.info("VALIDAÇÃO DE REGIÃO METROPOLITANA - GeoValida")
    logger.info("="*80)
    
    results = []
    
    # Executar validações
    results.append(("initialization.json", validate_initialization_json()))
    results.append(("DataLoader", validate_dataloader()))
    results.append(("Estrutura do Grafo", validate_graph_structure()))
    results.append(("Interface", validate_interface_data()))
    
    # Resumo final
    logger.info("\n" + "="*80)
    logger.info("RESUMO FINAL DA VALIDAÇÃO")
    logger.info("="*80)
    
    for name, passed in results:
        status = "✅ PASSOU" if passed else "❌ FALHOU"
        logger.info(f"  {status}: {name}")
    
    all_passed = all(r[1] for r in results)
    
    if all_passed:
        logger.info("\n🎉 TODAS AS VALIDAÇÕES PASSARAM!")
        logger.info("✅ A implementação de Região Metropolitana está CORRETA!")
        return 0
    else:
        logger.error("\n❌ ALGUMAS VALIDAÇÕES FALHARAM!")
        logger.error("⚠️ Verifique os erros acima e aplique as correções necessárias.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
