# src/interface/palette.py

"""
Configuração centralizada de paletas de cores para o GeoValida.
Edite a lista CUSTOM_PALETTE abaixo para alterar as cores do mapa.
"""

# Paleta padrão (Pastel) - Mantida como fallback
DEFAULT_PALETTE = [
    '#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3', '#fdb462', 
    '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd', '#ccebc5', '#ffed6f',
    '#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f', '#cab2d6', '#ffff99'
]

# ==============================================================================
# ÁREA DE PERSONALIZAÇÃO DO USUÁRIO
# ==============================================================================
# Adicione/Remova cores hexadecimais nesta lista.
# O sistema ciclará, através destas cores para pintar as UTPs vizinhas.
# Quanto mais cores, menor a chance de repetir cores em vizinhos.

CUSTOM_PALETTE = [
    # Exemplo: '#FF0000', '#00FF00', '#0000FF'
    # Se mantiver vazia ou comentar, o sistema usará a DEFAULT_PALETTE
]

# ==============================================================================

def get_palette():
    """Retorna a paleta ativa (Customizada se houver, senão Padrão)."""
    if CUSTOM_PALETTE:
        return CUSTOM_PALETTE
    return DEFAULT_PALETTE
