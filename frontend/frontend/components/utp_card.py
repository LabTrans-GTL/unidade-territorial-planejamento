import reflex as rx
from .report_card import report_card
from ..styles import TEXT_COLOR, TEXT_FONT, PAGE_COLOR

def utp_card(
    sede_name: str = "Florianópolis - SC",
    municipality_name: str = "Florianópolis",
    codibge: str = "4205407",
    summary: str = "X movimentações realizadas",
    reports: list[dict] = []
) -> rx.Component:
    return rx.flex(
        # --- HEADER 1: Info UTP ---
        rx.box(
            rx.text(
                "UTP",
                color=TEXT_COLOR["amarelo_brasil"],
                font_family=TEXT_FONT,
                font_size="24px",
                font_weight="900", # Extra bold
                text_align="left",
                padding="16px",
            ),
            width="100%",
            bg=PAGE_COLOR["azul_brasil"],
        ),

        # --- HEADER 2: Info Sede ---
        rx.box(
            rx.flex(
                 rx.vstack(
                    rx.text(
                        "Sede",
                        color=TEXT_COLOR["branco"],
                        font_family=TEXT_FONT,
                        font_size="14px",
                        font_weight="normal",
                        text_align="left",
                    ),
                    rx.text(
                        sede_name, # Dynamic Sede Name
                        color=TEXT_COLOR["branco"],
                        font_family=TEXT_FONT,
                        font_size="20px",
                        font_weight="bold",
                        text_align="left",
                    ),
                    align_items="start",
                    spacing="1",
                ),
                rx.spacer(),
                 rx.vstack(
                     rx.text(
                        "CODIBGE",
                        color=TEXT_COLOR["branco"],
                        font_family=TEXT_FONT,
                        font_size="14px",
                        font_weight="bold", # "CODIBGE" label seems bold in screenshot
                        text_align="right",
                    ),
                    rx.text(
                        codibge, # Dynamic CODIBGE
                        color=TEXT_COLOR["branco"],
                        font_family=TEXT_FONT,
                        font_size="20px",
                        font_weight="bold",
                        text_align="right",
                    ),
                    align_items="end",
                    spacing="1",
                ),
                direction="row",
                width="100%",
                justify="between",
                padding="16px",
            ),
            width="100%",
            bg=PAGE_COLOR["azul_bandeira"], # Lighter blue
        ),

        # --- BODY: Resumo ---
        rx.flex(
            rx.vstack(
                rx.text(
                    "Resumo",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="14px",
                    font_weight="bold",
                    text_align="left",
                    padding_bottom="4px",
                ),
                 rx.text(
                    summary,
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="14px",
                    font_weight="normal",
                    text_align="left",
                ),
                align_items="start",
                padding="24px", # Generous padding
                width="100%",
            ),
             rx.box(
                 rx.vstack(
                    *[
                        report_card(title=report["title"], reason=report["reason"])
                        for report in reports
                    ],
                    width="100%",
                    spacing="2",
                ),
                width="100%",
                padding_x="24px",
                padding_bottom="24px",
                overflow_y="auto",
                flex="1",
                css={
                    "&::-webkit-scrollbar": {
                        "width": "8px",
                    },
                    "&::-webkit-scrollbar-track": {
                        "background": "transparent",
                    },
                    "&::-webkit-scrollbar-thumb": {
                        "background": PAGE_COLOR["azul_bandeira"],
                        "border_radius": "4px",
                    },
                    "&::-webkit-scrollbar-thumb:hover": {
                        "background": PAGE_COLOR["azul_brasil"],
                    },
                }
            ),
            direction="column",
            width="100%",
            bg=PAGE_COLOR["branco"],
            flex="1",
            overflow="hidden",
        ),
        direction="column",
        width="100%",
        height="700px",
        border_radius="0px", # Rectangular design
        box_shadow="0px 4px 12px rgba(0, 0, 0, 0.1)",
        overflow="hidden",
    )


