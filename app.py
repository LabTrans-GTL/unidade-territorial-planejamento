# app.py (Raiz)
import sys
import time
from pathlib import Path

# Adicionar o diretório raiz do projeto ao sys.path
# Isso garante que os imports de 'src' funcionem tanto localmente quanto no Streamlit Cloud
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import streamlit as st
from src.core.manager import GeoValidaManager
from src.interface.dashboard import render_dashboard
from src.utils import DataLoader
import logging

# Configurar logger
logging.basicConfig(level=logging.WARNING)

# Inicializar manager apenas uma vez
@st.cache_resource(ttl=3600)
def get_manager():
    """Cria e inicializa o manager uma única vez"""
    start_time = time.time()
    logging.warning("🚀 Iniciando GeoValidaManager...")
    manager = GeoValidaManager()
    manager.step_0_initialize_data()
    end_time = time.time()
    logging.warning(f"✅ GeoValidaManager inicializado em {end_time - start_time:.2f} segundos")
    return manager

# Carregar dados do JSON para uso no dashboard
@st.cache_data(ttl=3600)
def load_json_data():
    """Cache do DataLoader"""
    start_time = time.time()
    logging.warning("🚀 Carregando dados do JSON...")
    loader = DataLoader()
    end_time = time.time()
    logging.warning(f"✅ Dados JSON carregados em {end_time - start_time:.2f} segundos")
    return loader

# Obter instâncias únicas
manager = get_manager()
data_loader = load_json_data()

# Renderizar dashboard
render_dashboard(manager)
