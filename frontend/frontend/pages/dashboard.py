import reflex as rx
from ..components.header import header
from ..components.sidebar import sidebar
from ..components.topbar import topbar
from ..components.map_card import map_card
from ..components.utp_card import utp_card
from ..state import MapState
from ..styles import PAGE_COLOR

def dashboard() -> rx.Component:
    return rx.vstack(
        header(),
        topbar(),
        rx.hstack(
            sidebar(),
            rx.box(
                rx.hstack(
                    # Left Column: Map (Takes more space)
                    rx.box(
                        map_card(),
                        width="65%",
                    ),
                    # Right Column: UTP Info (Takes less space)
                    rx.box(
                        utp_card(
                            codibge="4205407",
                            summary="45 movimentações realizadas",
                            reports=[
                                {
                                    "title": "Município 187878 removido",
                                    "reason": "O municipio de Canoinhas não pertencia a mesma região metropolitana"
                                },
                                {
                                    "title": "Município 187878 removido",
                                    "reason": "O municipio de Canoinhas não pertencia a mesma região metropolitana"
                                },
                                {
                                    "title": "Município 187878 removido",
                                    "reason": "O municipio de Canoinhas não pertencia a mesma região metropolitana"
                                },
                                {
                                    "title": "Município 187878 removido",
                                    "reason": "O municipio de Canoinhas não pertencia a mesma região metropolitana"
                                },
                            ]
                        ),
                        width="35%",
                    ),
                    spacing="6",
                    width="100%",
                    align_items="start",
                ),
                width="100%",
                padding="2em",
                bg=PAGE_COLOR["bg"],
                flex_grow="1", # Takes remaining height
                height="100%",
            ),
            width="100%",
            spacing="0",
            align_items="start",
            flex_grow="1",
            overflow="hidden", # Prevent scroll on the container itself if desired, or "auto"
        ),
        width="100%",
        spacing="0",
        height="100vh",
        overflow="auto", # Allow scrolling when content exceeds viewport (e.g. zoom)
        on_mount=MapState.generate_map,
    )
