import reflex as rx
from ..components.header import header
from ..components.login_card import login_card
from ..components.topbar import topbar_logo, topbar
from ..components.login_background import login_background
from ..styles import PAGE_COLOR, TEXT_COLOR, TEXT_FONT

def login() -> rx.Component:
    return rx.vstack(
        # 1. Barra gov.br
        header(),

        # 2. Barra de Título com Logo (Sub-header)
        topbar(),

        # 3. Área Principal com Fundo e Card de Login
        rx.box(
            # Camada de Fundo (Padrão Geométrico)
            rx.box(
                login_background(),
                position="absolute",
                top="0",
                left="0",
                width="100%",
                height="100%",
                z_index="0",
            ),
            
            # Camada de Conteúdo (Card) - Centralizado
            rx.center(
                login_card(),
                width="100%",
                height="100%",
                position="relative",
                z_index="1",
                padding="2em",
            ),
            
            width="100%",
            flex="1",
            position="relative",
            overflow="hidden",
        ),
        
        height="100vh",
        width="100%",
        spacing="0",
    )