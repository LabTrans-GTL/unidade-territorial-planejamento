import reflex as rx
from ..styles import TEXT_COLOR, TEXT_FONT, PAGE_COLOR

def report_card(title: str, reason: str) -> rx.Component:
    return rx.box(
        rx.vstack(
             rx.text(
                title,
                color=TEXT_COLOR["preto"],
                font_family=TEXT_FONT,
                font_size="14px",
                font_weight="bold",
                text_align="left",
            ),
            rx.box(
                rx.text(
                    "Motivo:",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="14px",
                    font_weight="bold",
                    as_="span",
                ),
                rx.text(
                    f" {reason}",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="14px",
                    font_weight="normal",
                    as_="span",
                ),
                text_align="left",
            ),
            align_items="start",
            spacing="2",
            padding="16px",
        ),
        width="100%",
        bg=PAGE_COLOR["branco"],
        border="1px solid #EAEAEA",
        border_radius="4px",
    )
