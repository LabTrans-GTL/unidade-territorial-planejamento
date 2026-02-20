#!/usr/bin/env python3
# main.py
"""
Script mestre do GeoValida.
Orquestra todo o processo desde a inicialização de dados até a consolidação final.
"""
import sys
import logging
from src.config import setup_logging

# Importar ferramentas
from scripts.s01_create_initialization import main as initialize_data
from src.run_consolidation import run_consolidation
from scripts.s03_precompute_coloring import main as precompute_coloring

# Configurar logger
logger = setup_logging()

def run_pipeline():
    """Executa o pipeline completo do GeoValida."""
    logger.info("*" * 80)
    logger.info("           GEOVALIDA - PIPELINE MASTER           ")
    logger.info("*" * 80)
    
    # 1. Inicializar Dados
    logger.info("\n>>> ETAPA 1: INICIALIZAÇÃO DE DADOS <<<")
    try:
        initialize_data()
        logger.info("✅ Inicialização de dados concluída.")
    except Exception as e:
        logger.error(f"❌ Falha na inicialização de dados: {e}")
        return False
    
    # 1.5 Enriquecer com dados socioeconômicos e turismo
    logger.info("\n>>> ETAPA 1.5: ENRIQUECIMENTO COM DADOS SOCIOECONÔMICOS <<<")
    try:
        from scripts.s04_enrich_initialization import main as enrich_data
        result = enrich_data()
        if result == 0:
            logger.info("✅ Enriquecimento de dados concluído.")
        else:
            logger.warning("⚠️ Erro no enriquecimento (não crítico).")
    except Exception as e:
        logger.warning(f"⚠️ Erro no enriquecimento: {e}")
        logger.info("   (Alguns dados socioeconômicos podem estar ausentes)")
        
    # 2. Executar Consolidação
    logger.info("\n>>> ETAPA 2: PROCESSAMENTO E CONSOLIDAÇÃO <<<")
    try:
        success = run_consolidation()
        if success:
            logger.info("✅ Consolidação concluída.")
        else:
            logger.error("❌ Falha na consolidação.")
            return False
    except Exception as e:
        logger.error(f"❌ Erro crítico na consolidação: {e}")
        return False
    
    # 3. Pré-calcular Coloração de Grafos
    logger.info("\n>>> ETAPA 3: PRÉ-CÁLCULO DE COLORAÇÃO DE GRAFOS <<<")
    try:
        result = precompute_coloring()
        if result == 0:
            logger.info("✅ Pré-cálculo de coloração concluído.")
        else:
            logger.warning("⚠️ Erro no pré-cálculo de coloração (não crítico).")
            # Não retornar False aqui, pois isso não impede o uso do sistema
    except Exception as e:
        logger.warning(f"⚠️ Erro no pré-cálculo de coloração: {e}")
        logger.info("   (O dashboard poderá calcular a coloração sob demanda)")
    
    # 4. Pré-processar GeoDataFrames
    logger.info("\n>>> ETAPA 4: PRÉ-PROCESSAMENTO DE GEODATAFRAMES <<<")
    try:
        from scripts.s05_preprocess_geodataframes import main as preprocess_geodataframes
        result = preprocess_geodataframes()
        if result == 0:
            logger.info("✅ Pré-processamento de GeoDataFrames concluído.")
        else:
            logger.warning("⚠️ Erro no pré-processamento (não crítico).")
    except Exception as e:
        logger.warning(f"⚠️ Erro no pré-processamento: {e}")
        logger.info("   (O dashboard poderá processar os shapefiles sob demanda)")
        
        
    # 5. Pré-calcular Popups de Fluxo
    logger.info("\n>>> ETAPA 5: PRÉ-CÁLCULO DE POPUPS DE FLUXO <<<")
    try:
        from scripts.s06_precompute_popups import main as precompute_popups
        result = precompute_popups()
        if result == 0:
            logger.info("✅ Pré-cálculo de popups concluído.")
        else:
            logger.warning("⚠️ Erro no pré-cálculo de popups (não crítico).")
    except Exception as e:
        logger.warning(f"⚠️ Erro no pré-cálculo de popups: {e}")
        logger.info("   (O dashboard calulará os popups sob demanda)")
        
    logger.info("\n" + "*" * 80)
    logger.info("✅ PIPELINE COMPLETO FINALIZADO COM SUCESSO")
    logger.info("*" * 80)
    return True

if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)


