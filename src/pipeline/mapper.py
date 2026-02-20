# src/pipeline/mapper.py
import logging
import matplotlib.pyplot as plt
import geopandas as gpd
from pathlib import Path


class UTPMapGenerator:
    def __init__(self, graph):
        self.graph = graph
        self.gdf_complete = None
        self.logger = logging.getLogger("GeoValida.Mapper")

    def load_shapefiles(self):
        """Carrega e prepara os arquivos geográficos.

        Tenta localizar o shapefile de municípios padrão dentro da configuração `FILES`.
        """
        from src.config import FILES

        shp_candidate = None
        try:
            # Preferimos um arquivo .shp com o nome padrão
            shp_candidate = Path(FILES['shapefiles']) / "BR_Municipios_2024.shp"
            if not shp_candidate.exists():
                # fallback para qualquer .shp dentro da pasta
                shp_dir = Path(FILES['shapefiles'])
                shp_files = list(shp_dir.glob('*.shp'))
                if shp_files:
                    shp_candidate = shp_files[0]
                else:
                    raise FileNotFoundError(f"Nenhum shapefile encontrado em {shp_dir}")

            self.gdf_complete = gpd.read_file(shp_candidate)
        except Exception as e:
            self.logger.error(f"Erro carregando shapefile de municípios: {e}")
            raise

    def sync_with_graph(self, graph):
        """Atualiza o GeoDataFrame com o estado atual do Grafo (quem pertence a qual UTP).

        Implementação vetorizada: constrói um dicionário `cd_mun -> utp_id` e aplica um `map` sobre
        a coluna `CD_MUN` do GeoDataFrame para atualizar `UTP_ID` de forma rápida.
        """
        if self.gdf_complete is None:
            raise RuntimeError("GDF não carregado. Execute `load_shapefiles()` antes.")

        if 'CD_MUN' not in self.gdf_complete.columns:
            raise RuntimeError("GeoDataFrame não contém coluna 'CD_MUN'.")
        
        # CRITICAL: Clean empty UTPs before syncing to ensure consistency
        removed = graph.cleanup_empty_utps()
        if removed > 0:
            self.logger.info(f"   Removed {removed} empty UTP nodes before sync")

        # Constrói mapeamento do grafo: município -> UTP
        utp_mapping = {}
        rm_mapping = {}
        
        for node, data in graph.hierarchy.nodes(data=True):
            if data.get('type') == 'municipality':
                mun_id = int(node)
                
                # RM Mapping
                rm_name = data.get('regiao_metropolitana')
                if rm_name:
                    rm_mapping[mun_id] = rm_name

                # encontra o pai UTP
                parents = list(graph.hierarchy.predecessors(node))
                utp_id = None
                for p in parents:
                    ps = str(p)
                    if ps.startswith('UTP_'):
                        utp_id = ps.replace('UTP_', '')
                        break
                if utp_id:
                    utp_mapping[mun_id] = utp_id

        self.logger.info(f"Mapeamento: {len(utp_mapping)} municípios → UTPs / {len(rm_mapping)} municípios → RMs")

        # Aplica mapeamento de forma vetorizada
        try:
            # Converte CD_MUN para int para matching correto
            self.gdf_complete['CD_MUN_int'] = self.gdf_complete['CD_MUN'].astype(int)
            self.gdf_complete['UTP_ID'] = self.gdf_complete['CD_MUN_int'].map(utp_mapping)
            self.gdf_complete['RM_NAME'] = self.gdf_complete['CD_MUN_int'].map(rm_mapping)
            
            # Remove coluna temporária
            self.gdf_complete.drop('CD_MUN_int', axis=1, inplace=True)
            
            # Preenche NAs com valor padrão
            self.gdf_complete['UTP_ID'] = self.gdf_complete['UTP_ID'].fillna('SEM_UTP')
            self.gdf_complete['RM_NAME'] = self.gdf_complete['RM_NAME'].fillna('SEM_RM')
            
            self.logger.info(f"Sincronização completa. UTPs únicas: {self.gdf_complete['UTP_ID'].nunique()}")
        except Exception as e:
            self.logger.error(f"Erro ao sincronizar: {e}")
            raise

        return self

    def save_map(self, output_path, title="Mapa UTP", column='UTP_ID'):
        """Gera e salva a imagem do mapa.
        
        Args:
            output_path (Path): Caminho para salvar a imagem.
            title (str): Título do mapa.
            column (str): Coluna a ser usada para colorir o mapa (padrão: 'UTP_ID').
        """
        if self.gdf_complete is None:
            raise RuntimeError("GDF não carregado. Execute `load_shapefiles()` antes.")

        fig, ax = plt.subplots(figsize=(15, 15))
        # Lógica de plotagem (cores por UTP, bordas de UF, etc)
        try:
            self.gdf_complete.plot(ax=ax, column=column, cmap='tab20', legend=True)
            # Remove legend if too many categories? For now keep default.
        except Exception as e:
            self.logger.error(f"Erro ao plotar o mapa: {e}")
            raise
        plt.title(title)
        plt.savefig(output_path)
        plt.close()

    def save_rm_map(self, output_path):
        """Gera um mapa específico das Regiões Metropolitanas, dissolvendo municípios."""
        self.logger.info(f"Gerando mapa de RMs em {output_path}...")
        
        if self.gdf_complete is None:
            raise RuntimeError("GDF não carregado.")
            
        try:
            # Filtra apenas quem tem RM
            gdf_rms = self.gdf_complete[self.gdf_complete['RM_NAME'] != 'SEM_RM'].copy()
            
            if gdf_rms.empty:
                self.logger.warning("Nenhuma RM encontrada para plotar.")
                return

            # Dissolve por Nome da RM para criar o contorno
            gdf_dissolved = gdf_rms.dissolve(by='RM_NAME')
            
            fig, ax = plt.subplots(figsize=(15, 15))
            
            # Plotar mapa base (cinza claro)
            base = self.gdf_complete.plot(ax=ax, color='#f0f0f0', edgecolor='white', linewidth=0.2)
            
            # Plotar RMs coloridas
            gdf_dissolved.plot(ax=ax, cmap='Set2', alpha=0.6, edgecolor='black', linewidth=0.5, legend=True)
            
            plt.title("Regiões Metropolitanas (Baseadas em Municípios)")
            plt.savefig(output_path)
            plt.close()
            self.logger.info("Mapa de RMs salvo com sucesso.")
            
        except Exception as e:
            self.logger.error(f"Erro ao gerar mapa de RMs: {e}")
            import traceback
            self.logger.error(traceback.format_exc())