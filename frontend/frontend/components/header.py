import reflex as rx
from ..styles import PAGE_COLOR

def header() -> rx.Component:
    return rx.hstack(
        rx.box(
            rx.heading("gov.br", size="6", color="white", weight="bold"),
        ),
        rx.spacer(),
        
        # Use rx.link para links, não rx.text
        rx.link("Dashboard", href="/", color="white", weight="regular", size="3"),
        rx.link("Labtrans", href="https://www.labtrans.ufsc.br/", color="white", weight="regular", size="3"),
        rx.link("Equipe", href="https://www.labtrans.ufsc.br/", color="white", weight="regular", size="3"),
        rx.link("Documentos", href="https://www.labtrans.ufsc.br/", color="white", weight="regular", size="3"),

        # Estilos:
        bg=PAGE_COLOR["azul_brasil"], 
        padding="1em",
        width="100%",
        align="center",
    )