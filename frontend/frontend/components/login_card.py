import reflex as rx
from ..styles import PAGE_COLOR, TEXT_COLOR, TEXT_FONT

class LoginState(rx.State):
    show_password: bool = False

    def toggle_password(self):
        self.show_password = not self.show_password

def login_card() -> rx.Component:
    return rx.box(
        # --- HEADER 1: Texto Amarelo ---
        rx.box(
            rx.text(
                "Unidade Territorial de Planejamento",
                color=TEXT_COLOR["azul_brasil"],
                font_family=TEXT_FONT,
                font_size="13px", 
                font_weight="medium",
                text_align="center",
                padding="16px",
            ),
            width="100%",
            bg=PAGE_COLOR["cinza_sidebar"],
            border_bottom="1px solid #EAEAEA",
        ),
        
        # --- HEADER 2: Bem Vindo ---
        rx.box(
            rx.text(
                "Bem Vindo",
                color=TEXT_COLOR["azul_brasil"],
                font_family=TEXT_FONT,
                font_size="32px", 
                font_weight="bold",
                text_align="center",
                padding="24px 0",
            ),
            width="100%",
            bg="white",
        ),
        
        # --- CORPO: Inputs e Botões ---
        rx.vstack(
            # Campo de Usuário
            rx.input(
                placeholder="usuário",
                width="100%",
                height="45px",
                variant="surface",
                border="1px solid #EAEAEA",
                bg="white",
                radius="medium",
                font_family=TEXT_FONT,
                _focus={
                    "border_color": TEXT_COLOR["azul_brasil"],
                    "outline": "none",
                },
                transition="all 0.2s ease-in-out",
            ),
            # Campo de Senha
            rx.box(
                rx.input(
                    rx.input.slot(
                        rx.icon(
                            tag=rx.cond(LoginState.show_password, "eye", "eye-off"),
                            on_click=LoginState.toggle_password,
                            cursor="pointer",
                            size=18,
                            color=TEXT_COLOR["azul_brasil"],
                        ),
                        side="right",
                    ),
                    type=rx.cond(LoginState.show_password, "text", "password"),
                    placeholder="senha",
                    width="100%",
                    height="45px",
                    variant="surface",
                    border="1px solid #EAEAEA",
                    bg="white",
                    radius="medium",
                    font_family=TEXT_FONT,
                    _focus={
                        "border_color": TEXT_COLOR["azul_brasil"],
                        "outline": "none",
                    },
                    transition="all 0.2s ease-in-out",
                ),
                width="100%",
            ),
            
            # Linha de Botões
            rx.hstack(
                rx.button(
                    "Entrar",
                    bg=TEXT_COLOR["azul_brasil"],
                    color="white",
                    width="100%",
                    flex="1",
                    height="45px",
                    font_weight="medium",
                    font_family=TEXT_FONT,
                    radius="medium",
                    _hover={
                        "bg": "rgba(7, 29, 65, 0.9)",
                        "transform": "translateY(-1px)",
                        "box_shadow": "0 4px 12px rgba(7, 29, 65, 0.15)",
                    },
                    transition="all 0.2s ease-in-out",
                ),
                rx.button(
                    "Acesso Público",
                    on_click=rx.redirect("/dashboard"),
                    bg="white",
                    color=TEXT_COLOR["azul_brasil"],
                    border=f"1px solid {TEXT_COLOR['azul_brasil']}",
                    width="100%",
                    flex="1",
                    height="45px",
                    font_weight="medium",
                    font_family=TEXT_FONT,
                    radius="medium",
                    _hover={
                        "bg": "rgba(7, 29, 65, 0.03)",
                        "transform": "translateY(-1px)",
                        "box_shadow": "0 4px 12px rgba(7, 29, 65, 0.08)",
                    },
                    transition="all 0.2s ease-in-out",
                ),
                width="100%",
                spacing="4",
                padding_top="8px",
                justify="center",
            ),
            width="100%",
            max_width="450px",
            spacing="4",
            padding="40px", 
            align_items="center",
            margin_x="auto",
        ),
        
        # Estilo do Card Principal
        width=["95%", "500px", "600px"],
        bg="white",
        border_radius="5px",
        border="1px solid #EAEAEA",
        box_shadow="0px 4px 20px rgba(0,0,0,0.06)",
        overflow="hidden", 
    )