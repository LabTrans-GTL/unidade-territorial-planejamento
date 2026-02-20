import reflex as rx
from ..styles import PAGE_COLOR, TEXT_COLOR, TEXT_FONT

def topbar_logo(image_path: str) -> rx.Component:
    return rx.image(
        src=image_path,
        width="60px", 
        height="auto",
    )

def topbar() -> rx.Component:
    return rx.box(
        rx.box(
            rx.hstack(
                # --- LADO ESQUERDO: LOGO + TÍTULO ---
                rx.hstack(
                    topbar_logo("/logo.png"),
                    rx.vstack(
                        rx.text(
                            "Unidade Territorial", 
                            font_family=TEXT_FONT, 
                            size="4", 
                            color=TEXT_COLOR["azul_brasil"], 
                            weight="bold",
                            line_height="1.2"
                        ),
                        rx.text(
                            "de Planejamento",
                            font_family=TEXT_FONT, 
                            size="4", 
                            color=TEXT_COLOR["azul_brasil"], 
                            weight="regular",
                            line_height="1.2"
                        ),
                        width=["100%", "auto"],
                        display=["none", "flex"], # Hide text on mobile
                        spacing="0", 
                        align_items="start",
                    ),
                    align="center",
                    spacing="3",
                ),

                rx.spacer(), 

                # --- CENTRO: BARRA DE BUSCA ---
                rx.input(
                    rx.input.slot(
                        rx.icon("search", size=18, color="gray")
                    ),
                    placeholder="Pesquisar UTPs ...",
                    width=["100%", "200px", "350px"],
                    radius="large",
                    bg="white",
                    _focus={
                        "border_color": f"{PAGE_COLOR['azul_bandeira']}80",
                        "box_shadow": f"0 0 0 1px {PAGE_COLOR['azul_bandeira']}80",
                    },
                ),

                rx.spacer(), 

                # --- LADO DIREITO: BOTÃO ESTADOS ---
                rx.button(
                    rx.icon("map-pin", size=18),
                    rx.text("Estados", weight="medium"),
                    variant="outline",
                    color=PAGE_COLOR["azul_brasil"],
                    border_color=PAGE_COLOR["azul_brasil"],
                    radius="large",
                    padding_x="1.5em",
                    _hover={"bg": "rgba(0,0,0, 0.1)"},
                ),

                align="center",
                width="100%",
            ),
            padding="1em 2em",
            height=["auto", "80px"], 
            width="100%",
            border_top_left_radius="30px",
            bg="white",
            border_bottom="1px solid #EAEAEA",
            display="flex",
            align_items="center",
        ),
        bg=PAGE_COLOR["azul_brasil"], # Cor do fundo da "curva"
        width="100%",
    )



    