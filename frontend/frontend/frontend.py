import reflex as rx
from .pages.login import login
from .pages.dashboard import dashboard

# importando os components para teste
from .components.header import header
from .components.sidebar import sidebar
from .components.topbar import topbar
from .components.login_card import login_card
from .components.utp_card import utp_card
from .components.map_card import map_card
from .test_popup import test_popup_flow

app = rx.App(
    stylesheets=[
        "https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;700&display=swap",
    ],
    head_components=[
        rx.el.link(rel="icon", href="/logo.png"),
    ],
)


def rascunho():
    return rx.center(
        rx.vstack(
            map_card(), # Novo componente
            utp_card(
                codibge="4205407",
                summary="X movimentações realizadas",
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
                    }

                ]
            ),
            spacing="4",
            width="100%",
            max_width="800px", # Limit width for better view
        ),
        width="100%",
        min_height="100vh", # Ensure full height
        bg="gray",
        padding="20px",
    )
app.add_page(rascunho, route="/teste")
app.add_page(test_popup_flow, route="/popup-flow")
app.add_page(login, route="/login")
app.add_page(dashboard, route="/dashboard")
