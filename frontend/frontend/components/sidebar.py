import reflex as rx
from ..styles import TEXT_COLOR, PAGE_COLOR, TEXT_FONT
from ..states import SidebarState

def sidebar_item(text: str, icon: str, url: str) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.icon(icon, size=20, color=TEXT_COLOR["azul_brasil"]),
            rx.box(
                rx.text(text, font_family=TEXT_FONT, size="3", weight="medium", color=TEXT_COLOR["azul_brasil"], white_space="nowrap"),
                opacity=rx.cond(SidebarState.show_sidebar, "1", "0"),
                transition="opacity 0.3s ease-in-out",
                width=rx.cond(SidebarState.show_sidebar, "auto", "0"),
                overflow="hidden",
                height="auto",
            ),
            spacing=rx.cond(SidebarState.show_sidebar, "3", "0"),
            align="center",
            justify=rx.cond(SidebarState.show_sidebar, "start", "center"),
            width="100%",
            padding="12px",
            border_radius="8px",
            _hover={
                "bg": "rgba(7, 29, 65, 0.05)",
            },
            transition="all 0.3s ease-in-out",
        ),
        href=url,
        width="100%",
        underline="none",
    )

def sidebar() -> rx.Component:
    return rx.box(
        rx.vstack(
            # Header with Toggle
            rx.hstack(
                rx.cond(
                    SidebarState.show_sidebar,
                    rx.heading("Menu", font_family=TEXT_FONT, size="5", color=TEXT_COLOR["azul_brasil"]),
                ),
                rx.cond(
                    SidebarState.show_sidebar,
                    rx.spacer(),
                ),
                rx.icon_button(
                    rx.icon("menu"),
                    on_click=SidebarState.toggle_sidebar,
                    variant="ghost",
                    color=TEXT_COLOR["azul_brasil"],
                    cursor="pointer",
                    _hover={"bg": "transparent"},
                ),
                width="100%",
                align="center",
                padding_bottom="2em",
                padding_x="10px",
                justify=rx.cond(SidebarState.show_sidebar, "between", "center"),
            ),
            
            # Menu Items
            rx.vstack(
                sidebar_item("Dashboard", "layout-dashboard", "/dashboard"),
                sidebar_item("Relatório", "bar-chart-2", "/report"),
                sidebar_item("Configurações", "settings", "/settings"),
                spacing="2",
                width="100%",
            ),
            rx.spacer(),

            # Footer
            rx.vstack(
                rx.divider(border_color=TEXT_COLOR["preto"], width="80%", opacity="0.2"), 
                rx.box(
                    rx.text(
                        "Labtrans - UFSC",
                        font_family=TEXT_FONT,
                        size="1",
                        weight="light",
                        color=TEXT_COLOR["azul_brasil"],
                        padding_top="2px",
                        text_align="center",
                        white_space="nowrap"
                    ),
                    opacity=rx.cond(SidebarState.show_sidebar, "1", "0"),
                    transition="opacity 0.3s ease-in-out",
                    width=rx.cond(SidebarState.show_sidebar, "auto", "0"),
                    overflow="hidden",
                ),
                width="100%",
                align="center",
                padding_bottom="2em",
            ),
            
            height="100%",
            width="100%",
            justify="between", 
        ),
        padding="1em",
        height="100%",
        bg=PAGE_COLOR["cinza_sidebar"],
        width=rx.cond(SidebarState.show_sidebar, "280px", "80px"),
        display=["none", "none", "block"], 
        border_right="1px solid #EAEAEA",
        transition="width 0.3s ease-in-out",
    )
