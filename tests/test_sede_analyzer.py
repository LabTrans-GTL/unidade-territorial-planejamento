# tests/test_sede_analyzer.py
"""
Testes para o módulo SedeAnalyzer.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.pipeline.sede_analyzer import SedeAnalyzer


def test_sede_analyzer_initialization():
    """Testa a inicialização do SedeAnalyzer."""
    analyzer = SedeAnalyzer()
    assert analyzer is not None
    assert analyzer.df_municipios is None
    assert analyzer.df_impedance is None


def test_load_initialization_data():
    """Testa o carregamento de dados do initialization.json."""
    analyzer = SedeAnalyzer()
    
    # Tentar carregar dados
    success = analyzer.load_initialization_data()
    
    # Se o arquivo existir, deve carregar com sucesso
    json_path = Path(__file__).parent.parent / "data" / "initialization.json"
    if json_path.exists():
        assert success is True
        assert analyzer.df_municipios is not None
        assert len(analyzer.df_municipios) > 0
    else:
        assert success is False


def test_load_impedance_data():
    """Testa o carregamento da matriz de impedância."""
    analyzer = SedeAnalyzer()
    
    # Tentar carregar impedância
    success = analyzer.load_impedance_data()
    
    # Se o arquivo existir, deve carregar com sucesso
    impedance_path = Path(__file__).parent.parent / "data" / "01_raw" / "impedance" / "impedancias_filtradas_2h.csv"
    if impedance_path.exists():
        assert success is True
        assert analyzer.df_impedance is not None
        assert len(analyzer.df_impedance) > 0
        # Verificar colunas esperadas
        assert 'origem' in analyzer.df_impedance.columns
        assert 'destino' in analyzer.df_impedance.columns
        assert 'tempo_horas' in analyzer.df_impedance.columns


def test_analyze_sede_dependencies():
    """Testa a análise completa de dependências."""
    analyzer = SedeAnalyzer()
    
    # Executar análise
    summary = analyzer.analyze_sede_dependencies()
    
    # Verificar estrutura do resultado
    assert isinstance(summary, dict)
    assert 'success' in summary
    
    # Se bem-sucedido, verificar métricas
    if summary['success']:
        assert 'total_sedes' in summary
        assert 'total_alertas' in summary
        assert 'utps_analisadas' in summary
        assert summary['total_sedes'] >= 0
        assert summary['total_alertas'] >= 0


def test_export_sede_comparison_table():
    """Testa a exportação da tabela comparativa."""
    analyzer = SedeAnalyzer()
    
    # Executar análise primeiro
    summary = analyzer.analyze_sede_dependencies()
    
    if summary['success']:
        # Exportar tabela
        df = analyzer.export_sede_comparison_table()
        
        # Verificar estrutura
        assert isinstance(df, pd.DataFrame)
        
        # Se houver dados, verificar colunas esperadas
        if len(df) > 0:
            expected_columns = ['UTP', 'Sede', 'UF', 'REGIC', 'População']
            for col in expected_columns:
                assert col in df.columns


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
