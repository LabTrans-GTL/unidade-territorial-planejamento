# run_influence_analysis.py
import logging
import sys
from pathlib import Path
from src.pipeline.influence_analyzer import InfluenceAnalyzer
from src.config import setup_logging

def main():
    # Configurar logging
    setup_logging()
    logger = logging.getLogger("GeoValida.RunInfluence")
    
    try:
        # Inicializar analisador
        analyzer = InfluenceAnalyzer()
        
        # Carregar dados
        analyzer.load_data()
        
        # Rodar análise
        analyzer.run_analysis()
        
        # Exportar resultados
        output_dir = Path("data/03_processed")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "analise_hierarquia_influencia.csv"
        analyzer.export_results(output_file)
        
        # Mostrar resumo
        df = analyzer.get_results_df()
        resumo = df['hierarquia'].value_counts()
        print("\n=== RESUMO DA ANÁLISE DE INFLUÊNCIA ===")
        print(resumo)
        print(f"========================================")
        print(f"Resultados salvos em: {output_file}")
        
    except Exception as e:
        logger.error(f"Erro durante a execução: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()
