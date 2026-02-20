import reflex as rx
from ..styles import TEXT_COLOR, TEXT_FONT, PAGE_COLOR
from ..state import MapState


def map_card() -> rx.Component:
    """
    Map card component powered by amCharts 5 (via iframe).

    Controls:
    - Zoom In / Out / Home: handled natively inside the iframe by amCharts ZoomControl
    - Version selector: bottom bar triggers MapState.set_version → regenerates HTML
    - postMessage bridge: parent can send {action:"homeView"} etc. to iframe (future use)
    """
    return rx.flex(
        # ── Map Area ──────────────────────────────────────────────────
        rx.box(
            rx.cond(
                MapState.is_generating,
                # Loading state while HTML is being generated server-side
                rx.center(
                    rx.vstack(
                        rx.spinner(
                            color=TEXT_COLOR["amarelo_brasil"],
                            size="3",
                        ),
                        rx.text(
                            "Gerando mapa...",
                            color=TEXT_COLOR["azul_brasil"],
                            font_family=TEXT_FONT,
                            font_size="14px",
                            font_weight="600",
                        ),
                        spacing="3",
                        align_items="center",
                    ),
                    width="100%",
                    height="100%",
                    bg=PAGE_COLOR["mapa_bg"],
                ),
                # amCharts iframe — fills the entire map area
                # The amCharts ZoomControl is rendered inside the iframe
                rx.el.iframe(
                    src=MapState.map_url,
                    id="amcharts-map-frame",
                    width="100%",
                    height="100%",
                    style={
                        "border": "none",
                        "display": "block",
                    },
                ),
            ),
            width="100%",
            flex="1",
            position="relative",
            bg=PAGE_COLOR["mapa_bg"],
        ),

        # ── Bottom Bar ────────────────────────────────────────────────
        rx.flex(
            # Left: Metodologia link
            rx.hstack(
                rx.icon(tag="book-open", color=TEXT_COLOR["amarelo_brasil"], size=16),
                rx.text(
                    "Metodologia",
                    color=TEXT_COLOR["branco"],
                    font_family=TEXT_FONT,
                    font_weight="600",
                    font_size="14px",
                ),
                spacing="2",
                align_items="center",
                padding="10px 16px",
                cursor="pointer",
                border_radius="6px",
                _hover={
                    "bg": "rgba(255, 255, 255, 0.1)",
                    "transform": "translateY(-1px)",
                },
                transition="all 0.2s ease",
            ),

            rx.spacer(),

            # Center: Current version badge
            rx.hstack(
                rx.icon(tag="map-pin", color=TEXT_COLOR["amarelo_brasil"], size=14),
                rx.text(
                    f"Versão {MapState.current_version}",
                    color=TEXT_COLOR["amarelo_brasil"],
                    font_family=TEXT_FONT,
                    font_size="13px",
                    font_weight="700",
                ),
                spacing="2",
                align_items="center",
                bg="rgba(234, 205, 4, 0.15)",
                padding="6px 14px",
                border_radius="20px",
            ),

            rx.spacer(),

            # Right: Version selector dropdown
            rx.hstack(
                rx.text(
                    "Etapa:",
                    color=TEXT_COLOR["branco"],
                    font_family=TEXT_FONT,
                    font_weight="500",
                    font_size="13px",
                    opacity="0.8",
                ),
                rx.menu.root(
                    rx.menu.trigger(
                        rx.button(
                            rx.hstack(
                                rx.text(
                                    f"v{MapState.current_version}",
                                    font_weight="700",
                                    font_size="13px",
                                ),
                                rx.icon(tag="chevron-up", size=13),
                                spacing="2",
                                align_items="center",
                            ),
                            bg="rgba(255, 255, 255, 0.15)",
                            color=TEXT_COLOR["branco"],
                            font_family=TEXT_FONT,
                            padding="8px 14px",
                            border_radius="20px",
                            border="1px solid rgba(255, 255, 255, 0.25)",
                            cursor="pointer",
                            _hover={
                                "bg": "rgba(255, 255, 255, 0.25)",
                                "border_color": "rgba(255, 255, 255, 0.5)",
                            },
                            transition="all 0.2s ease",
                        ),
                    ),
                    rx.menu.content(
                        # v8.0 → step1 (Initial)
                        rx.menu.item(
                            rx.hstack(
                                rx.icon(tag="circle", size=8),
                                rx.text("v8.0 — Inicial"),
                                spacing="2",
                            ),
                            on_click=MapState.set_version("8.0"),
                        ),
                        # v8.1 → step5 (Post-unitary)
                        rx.menu.item(
                            rx.hstack(
                                rx.icon(tag="circle", size=8),
                                rx.text("v8.1 — Pós-unitário"),
                                spacing="2",
                            ),
                            on_click=MapState.set_version("8.1"),
                        ),
                        # v8.2 → step6 (Sede consolidation)
                        rx.menu.item(
                            rx.hstack(
                                rx.icon(tag="circle", size=8),
                                rx.text("v8.2 — Consolidação Sede"),
                                spacing="2",
                            ),
                            on_click=MapState.set_version("8.2"),
                        ),
                        # v8.3 → step8 (Final)
                        rx.menu.item(
                            rx.hstack(
                                rx.icon(tag="check-circle", size=8),
                                rx.text("v8.3 — Final"),
                                spacing="2",
                            ),
                            on_click=MapState.set_version("8.3"),
                        ),
                        rx.menu.separator(),
                        rx.menu.item(
                            rx.text(
                                "Zoom: use controles no mapa",
                                font_size="11px",
                                color="gray",
                                font_style="italic",
                            ),
                            disabled=True,
                        ),
                        bg=PAGE_COLOR["branco"],
                        color=TEXT_COLOR["azul_brasil"],
                        font_family=TEXT_FONT,
                        border_radius="12px",
                        box_shadow="0px 8px 24px rgba(0, 0, 0, 0.15)",
                        padding="8px",
                        side="top",
                        align="end",
                    ),
                ),
                spacing="2",
                align_items="center",
                padding="6px 12px",
            ),

            width="100%",
            bg=PAGE_COLOR["azul_brasil"],
            justify="between",
            align="center",
            padding="8px 16px",
            box_shadow="0px -2px 8px rgba(0, 0, 0, 0.12)",
            z_index="10",
        ),

        # ── Outer container ───────────────────────────────────────────
        direction="column",
        width="100%",
        height="700px",
        border_radius="12px",
        overflow="hidden",
        box_shadow="0px 8px 24px rgba(0, 0, 0, 0.12)",
        on_mount=MapState.generate_map,
        class_name="map-card-container",
    )
