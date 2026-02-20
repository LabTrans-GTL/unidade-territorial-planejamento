#!/usr/bin/env python3
# src/run_consolidation.py
"""
Script para executar a consolidação de UTPs e gerar o cache
Execute com: python src/run_consolidation.py
"""
import sys
import json
import pandas as pd
from pathlib import Path

# Adicionar raiz do projeto ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.manager import GeoValidaManager
from src.interface.consolidation_manager import ConsolidationManager
from src.interface.consolidation_loader import ConsolidationLoader
import logging

# Configurar logging para debug
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ConsolidationRunner")


def run_consolidation():
    """Executa o pipeline de consolidação completo."""
    logger.info("=" * 80)
    logger.info("INICIANDO EXECUÇÃO DE CONSOLIDAÇÃO DE UTPS")
    logger.info("=" * 80)
    
    try:
        # 1. Inicializar manager
        logger.info("\n1️⃣ Inicializando manager...")
        manager = GeoValidaManager()
        
        # 2. Carregar dados (Etapa 0)
        logger.info("\n2️⃣ Etapa 0: Carregando dados...")
        try:
            if not manager.step_0_initialize_data():
                logger.error("❌ Falha ao carregar dados!")
                return False
            logger.info("✅ Dados carregados com sucesso!")
        except Exception as e:
            logger.error(f"❌ Erro durante carregamento de dados: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        # 3. Gerar mapa inicial (Etapa 1)
        logger.info("\n3️⃣ Etapa 1: Gerando mapa inicial...")
        try:
            manager.step_1_generate_initial_map()
            logger.info("✅ Mapa inicial gerado!")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao gerar mapa inicial: {e}")
        
        # 4. Analisar fluxos (Etapa 2)
        logger.info("\n4️⃣ Etapa 2: Analisando fluxos...")
        try:
            manager.step_2_analyze_flows()
            logger.info("✅ Fluxos analisados!")
        except Exception as e:
            logger.warning(f"⚠️ Erro ao analisar fluxos: {e}")
        
        # 5. Consolidação Funcional (Etapa 5)
        logger.info("\n5️⃣ Etapa 5: Consolidação funcional...")
        try:
            changes_5 = manager.step_5_consolidate_functional()
            logger.info(f"✅ {changes_5} consolidações funcionais realizadas!")
        except Exception as e:
            logger.warning(f"⚠️ Erro na consolidação funcional: {e}")
            changes_5 = 0
        
        # 6. Limpeza Territorial (Etapa 7)
        logger.info("\n6️⃣ Etapa 7: Limpeza territorial...")
        try:
            changes_7 = manager.step_7_territorial_cleanup()
            logger.info(f"✅ {changes_7} limpezas territoriais realizadas!")
        except Exception as e:
            logger.warning(f"⚠️ Erro na limpeza territorial: {e}")
            changes_7 = 0

        # 6.5. Salvar snapshot pós-consolidação de unitárias (ANTES da consolidação de sedes)
        logger.info("\n📸 Salvando snapshot pós-consolidação de unitárias...")
        try:
            # Carregar consolidations até este ponto (Steps 5 + 7)
            consolidation_manager_snapshot = ConsolidationManager()
            
            # Criar loader e atualizar com dados até Step 7
            snapshot_loader = ConsolidationLoader()
            snapshot_loader.update_from_log(consolidation_manager_snapshot.log_data)
            
            # Salvar em arquivo separado
            post_unitary_path = Path(project_root) / "data" / "post_unitary_consolidation.json"
            with open(post_unitary_path, 'w', encoding='utf-8') as f:
                json.dump(snapshot_loader.result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✅ Snapshot salvo em: {post_unitary_path}")
            logger.info(f"   📊 {snapshot_loader.result['total_consolidations']} consolidações (Steps 5+7)")

            # --- GERAÇÃO DE CACHE DE COLORAÇÃO (PÓS-UNITÁRIAS) ---
            logger.info("   🎨 Gerando cache de coloração pós-unitárias (consolidated_coloring.json)...")
            try:
                # Sincronizar mapa com estado atual do grafo (Steps 5+7)
                manager.map_generator.sync_with_graph(manager.graph)
                gdf_step_5_7 = manager.map_generator.gdf_complete
                
                if gdf_step_5_7 is not None and not gdf_step_5_7.empty:
                    # Garantir coluna UTP_ID
                    if 'utp_id' in gdf_step_5_7.columns and 'UTP_ID' not in gdf_step_5_7.columns:
                        gdf_step_5_7['UTP_ID'] = gdf_step_5_7['utp_id']
                    
                    # Calcular cores
                    coloring_5_7 = manager.graph.compute_graph_coloring(gdf_step_5_7)
                    
                    # Salvar
                    coloring_path_5_7 = Path(project_root) / "data" / "consolidated_coloring.json"
                    with open(coloring_path_5_7, "w") as f:
                        json.dump(coloring_5_7, f)
                    logger.info(f"   ✅ Cache de coloração salvo: {len(coloring_5_7)} municípios")
                else:
                    logger.warning("   ⚠️ GDF vazio, pulando coloração.")
            except Exception as e_color:
                logger.warning(f"   ⚠️ Erro ao gerar coloração: {e_color}")
            # --------------------------------------------------------

        except Exception as e:
            logger.warning(f"⚠️ Erro ao salvar snapshot pós-unitárias: {e}")

        # 7. Consolidação de Sedes (Nova Etapa)
        logger.info("\n7️⃣ Etapa 6: Consolidação de Sedes (SedeConsolidator)...")
        try:
            changes_sedes = manager.step_6_consolidate_sedes()
            logger.info(f"✅ {changes_sedes} consolidações de sedes realizadas!")
            
            # --- GERAÇÃO DE CACHE DE COLORAÇÃO (PÓS-SEDES) ---
            if changes_sedes > 0: # Otimização: calcular apenas se houve mudanças, mas para garantir consistência melhor calcular sempre
                logger.info("   🎨 Gerando cache de coloração pós-sedes (post_sede_coloring.json)...")
                try:
                    # Sincronizar mapa com estado atual do grafo (Step 6)
                    manager.map_generator.sync_with_graph(manager.graph)
                    gdf_step_6 = manager.map_generator.gdf_complete
                    
                    if gdf_step_6 is not None and not gdf_step_6.empty:
                        if 'utp_id' in gdf_step_6.columns and 'UTP_ID' not in gdf_step_6.columns:
                            gdf_step_6['UTP_ID'] = gdf_step_6['utp_id']
                        
                        coloring_6 = manager.graph.compute_graph_coloring(gdf_step_6)
                        
                        coloring_path_6 = Path(project_root) / "data" / "post_sede_coloring.json"
                        with open(coloring_path_6, "w") as f:
                            json.dump(coloring_6, f)
                        logger.info(f"   ✅ Cache de coloração salvo: {len(coloring_6)} municípios")
                except Exception as e_color:
                    logger.warning(f"   ⚠️ Erro ao gerar coloração sedes: {e_color}")
            # ----------------------------------------------------

        except Exception as e:
            logger.warning(f"⚠️ Erro na consolidação de sedes: {e}")
            changes_sedes = 0
            import traceback
            logger.error(traceback.format_exc())

        # 8. Validação de Fronteiras (Etapa 8)
        logger.info("\n8️⃣ Etapa 8: Validação de Fronteiras...")
        try:
            changes_borders = manager.step_8_border_validation()
            logger.info(f"✅ {changes_borders} realocações de fronteira realizadas!")
        except Exception as e:
            logger.warning(f"⚠️ Erro na validação de fronteiras: {e}")
            changes_borders = 0
            import traceback
            logger.error(traceback.format_exc())
        
        # Sanitize returns
        changes_5 = changes_5 or 0
        changes_7 = changes_7 or 0
        changes_sedes = changes_sedes or 0
        changes_borders = changes_borders or 0
        
        total_changes = changes_5 + changes_7 + changes_sedes + changes_borders
        logger.info(f"✅ Total: {total_changes} (Step 5: {changes_5}, Step 6: {changes_sedes}, Step 7: {changes_7}, Step 8: {changes_borders})")
        
        # 7. Obter consolidation_manager do consolidator
        logger.info(f"\n7️⃣ Salvando {total_changes} consolidações em cache...")
        
        # Carregar o log completo do disco (incluindo Step 5, 7 e Sede)
        consolidation_manager = ConsolidationManager()
        
        # 8. Atualizar cache do loader
        logger.info("\n8️⃣ Atualizando cache de consolidação...")
        consolidation_loader = ConsolidationLoader()
        consolidation_loader.update_from_log(consolidation_manager.log_data)
        logger.info("✅ Cache atualizado!")
        
        # 9. Executar análise de dependências e cachear
        logger.info("\n9️⃣ Executando análise de dependências...")
        try:
            from src.pipeline.sede_analyzer import SedeAnalyzer
            # Imports moved to top
            # from pathlib import Path
            
            # Criar analisador
            analyzer = SedeAnalyzer(consolidation_loader=consolidation_loader)
            
            # --- CRITICAL FIX: INJECT CURRENT STATE ---
            # Instead of letting analyzer reload from initialization.json (which is old),
            # we construct the dataframe representing the PRESENT state (after Step 8).
            
            # 1. Get base socioeconomic data
            df_base = manager.municipios_data.copy()
            
            # 2. Get current territorial state (UTP_IDs corrected by Step 8)
            # We use manager.graph to be sure, or gdf_complete
            current_state = []
            for cd_mun in df_base['cd_mun']:
                # Get current UTP from graph
                utp_id = manager.graph.get_municipality_utp(cd_mun)
                
                # Get sede status from graph
                is_sede = False
                if manager.graph.hierarchy.has_node(cd_mun):
                    is_sede = manager.graph.hierarchy.nodes[cd_mun].get('sede_utp', False)
                
                current_state.append({
                    'cd_mun': cd_mun,
                    'utp_id_step9': utp_id,
                    'sede_utp_step9': is_sede
                })
            
            df_state = pd.DataFrame(current_state)
            
            # 3. Merge to update UTP_ID and SEDE_UTP
            # We assume cd_mun is unique INT in both
            df_final = df_base.merge(df_state, on='cd_mun', how='left')
            
            # Overwrite columns
            df_final['utp_id'] = df_final['utp_id_step9'].fillna(df_final['utp_id'])
            df_final['sede_utp'] = df_final['sede_utp_step9'].fillna(df_final['sede_utp'])
            
            # Drop temp columns
            df_final.drop(columns=['utp_id_step9', 'sede_utp_step9'], inplace=True)
            
            # 4. Inject into analyzer
            analyzer.df_municipios = df_final
            logger.info(f"   💉 Injected current state into SedeAnalyzer: {len(df_final)} municipalities")
            # ------------------------------------------

            # Executar análise
            sede_summary = analyzer.analyze_sede_dependencies()
            
            if sede_summary.get('success'):
                # Exportar para JSON
                cache_path = Path(project_root) / "data" / "sede_analysis_cache.json"
                if analyzer.export_to_json(cache_path):
                    logger.info(f"✅ Análise de dependências salva em: {cache_path}")
                    logger.info(f"   📊 {sede_summary['total_sedes']} sedes, {sede_summary['total_alertas']} alertas")
                else:
                    logger.warning("⚠️ Falha ao salvar análise de dependências")
            else:
                logger.warning(f"⚠️ Análise de dependências falhou: {sede_summary.get('error', 'Erro desconhecido')}")
        
        except Exception as e:
            logger.warning(f"⚠️ Erro ao executar análise de dependências: {e}")
        

        # 10. Gerar Cache de Coloração do Mapa (Otimização de Startup)
        logger.info("\n🔟 Gerando cache de coloração do mapa...")
        try:
            # import json (moved to top)
            
            # 11. Gerar Cache de Coloração do Mapa (Final - Pós-Validação)
            manager.map_generator.sync_with_graph(manager.graph) # <--- FIX: Sincronizar após Step 8
            gdf_complete = manager.map_generator.gdf_complete
            
            if gdf_complete is not None and not gdf_complete.empty:
                # O método compute_graph_coloring espera as colunas UTP_ID e CD_MUN
                # Vamos preparar um GDF compatível
                gdf_for_coloring = gdf_complete.copy()
                
                # Garantir nomes de colunas esperados pelo graph.py
                # O graph.py usa: row['CD_MUN'] e row['UTP_ID'] ou row['utp_id']
                if 'utp_id' in gdf_for_coloring.columns and 'UTP_ID' not in gdf_for_coloring.columns:
                    gdf_for_coloring['UTP_ID'] = gdf_for_coloring['utp_id']
                
                # Calcular coloração
                coloring = manager.graph.compute_graph_coloring(gdf_for_coloring)
                
                # Salvar em arquivo
                coloring_cache_path = Path(project_root) / "data" / "map_coloring_cache.json"
                with open(coloring_cache_path, "w") as f:
                    json.dump(coloring, f)
                    
                logger.info(f"✅ Cache de coloração salvo em: {coloring_cache_path}")
                logger.info(f"   🎨 {len(coloring)} municípios coloridos")
            else:
                logger.warning("⚠️ GDF vazio, pulando cache de coloração.")
                
        except Exception as e:
            logger.warning(f"⚠️ Erro ao gerar cache de coloração: {e}")
            # Não falha o pipeline por isso
        
        # 11. Exibir resumo
        logger.info("\n" + "=" * 80)
        logger.info("RESUMO DA EXECUÇÃO")
        logger.info("=" * 80)
        
        summary = consolidation_loader.get_summary()
        logger.info(f"Status: {summary['status']}")
        logger.info(f"Total de consolidações: {summary['total_consolidations']}")
        logger.info(f"UTPs de origem: {summary['unique_sources']}")
        logger.info(f"UTPs de destino: {summary['unique_targets']}")
        logger.info(f"Último update: {summary['timestamp']}")
        
        logger.info("\n✅ CONSOLIDAÇÃO CONCLUÍDA COM SUCESSO!")
        logger.info(f"📁 Arquivos gerados:")
        logger.info(f"   - data/consolidation_log.json (Log detalhado)")
        logger.info(f"   - data/consolidation_result.json (Cache rápido)")
        logger.info(f"   - data/map_coloring_cache.json (Cache de renderização)")
        logger.info(f"   - data/sede_analysis_cache.json (Cache de análise)")
        logger.info("\n💡 Dica: Recarregue o Streamlit para ver os resultados!")
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ ERRO DURANTE A EXECUÇÃO: {type(e).__name__}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = run_consolidation()
    sys.exit(0 if success else 1)
