import reflex as rx
from ..styles import TEXT_COLOR, TEXT_FONT, PAGE_COLOR


def destination_row(
    rank: int,
    dest_name: str,
    dest_uf: str,
    dest_cd: str,
    dest_utp: str,
    dest_rm: str,
    dest_pop: int,
    dest_regic: str,
    flow_count: int,
    percentage: float,
    tempo_horas: float = None
) -> rx.Component:
    """Single destination row in the flow table."""
    
    # Format population
    pop_fmt = f"{dest_pop:,}".replace(",", ".") if dest_pop else "-"
    
    # Format flow count
    flow_fmt = f"{flow_count:,}".replace(",", ".")
    
    # Format travel time
    if tempo_horas is not None:
        if tempo_horas < 1.0:
            tempo_str = f"{int(tempo_horas * 60)}min"
        else:
            tempo_str = f"{tempo_horas:.1f}h"
    else:
        tempo_str = "-"
    
    return rx.box(
        rx.hstack(
            # Rank
            rx.text(
                f"#{rank}",
                color=TEXT_COLOR["azul_brasil"],
                font_family=TEXT_FONT,
                font_size="14px",
                font_weight="bold",
                width="40px",
            ),
            
            # Municipality Name
            rx.vstack(
                rx.text(
                    f"{dest_name} ({dest_uf})",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="14px",
                    font_weight="bold",
                ),
                rx.text(
                    f"Cód: {dest_cd}",
                    color="#666",
                    font_family=TEXT_FONT,
                    font_size="11px",
                ),
                align_items="start",
                spacing="0",
                flex="1",
            ),
            
            # Territorial Data
            rx.vstack(
                rx.text(
                    f"UTP: {dest_utp}",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="11px",
                ),
                rx.text(
                    f"RM: {dest_rm[:20]}..." if len(str(dest_rm)) > 20 else f"RM: {dest_rm}",
                    color="#666",
                    font_family=TEXT_FONT,
                    font_size="10px",
                ),
                align_items="start",
                spacing="0",
                width="120px",
            ),
            
            # Social Data
            rx.vstack(
                rx.text(
                    f"Pop: {pop_fmt}",
                    color=TEXT_COLOR["preto"],
                    font_family=TEXT_FONT,
                    font_size="11px",
                ),
                rx.text(
                    dest_regic[:15] if len(str(dest_regic)) > 15 else dest_regic,
                    color="#666",
                    font_family=TEXT_FONT,
                    font_size="10px",
                ),
                align_items="start",
                spacing="0",
                width="100px",
            ),
            
            # Flow Count
            rx.text(
                flow_fmt,
                color=TEXT_COLOR["azul_brasil"],
                font_family=TEXT_FONT,
                font_size="14px",
                font_weight="bold",
                text_align="right",
                width="80px",
            ),
            
            # Percentage & Time
            rx.vstack(
                rx.text(
                    f"{percentage:.1f}%",
                    color=TEXT_COLOR["azul_bandeira"],
                    font_family=TEXT_FONT,
                    font_size="13px",
                    font_weight="bold",
                ),
                rx.text(
                    tempo_str,
                    color="#666",
                    font_family=TEXT_FONT,
                    font_size="11px",
                ),
                align_items="end",
                spacing="0",
                width="70px",
            ),
            
            spacing="3",
            width="100%",
            align="center",
            padding="12px",
        ),
        width="100%",
        bg=PAGE_COLOR["branco"] if rank % 2 == 0 else "#f9f9f9",
        border_bottom=f"1px solid #eee",
    )


def popup_flow(
    nm_mun: str = "Município",
    cd_mun: str = "0000000",
    uf: str = "UF",
    utp_id: str = "-",
    regiao_metropolitana: str = "-",
    regic: str = "-",
    populacao: int = 0,
    total_viagens: int = 0,
    top_destinations: list = None
) -> rx.Component:
    """
    Flow information popup component.
    
    Args:
        nm_mun: Municipality name
        cd_mun: IBGE code
        uf: State abbreviation
        utp_id: UTP identifier
        regiao_metropolitana: Metropolitan region name
        regic: REGIC classification
        populacao: Population count
        total_viagens: Total flow trips
        top_destinations: List of destination tuples (cd, name, uf, utp, rm, pop, regic, flow, pct, tempo)
    """
    
    # Format numbers
    pop_fmt = f"{populacao:,}".replace(",", ".") if populacao else "-"
    viagens_fmt = f"{total_viagens:,}".replace(",", ".") if total_viagens else "0"
    
    # Handle None/NaN values
    if not regiao_metropolitana or str(regiao_metropolitana).lower() in ['nan', 'none', '']:
        regiao_metropolitana = "-"
    if not regic or str(regic).lower() in ['nan', 'none', '', '0', '0.0']:
        regic = "-"
    
    # Default empty list if None
    if top_destinations is None:
        top_destinations = []
    
    return rx.flex(
        # --- HEADER 1: Municipality Title ---
        rx.box(
            rx.text(
                f"{nm_mun} ({uf})",
                color=TEXT_COLOR["branco"],
                font_family=TEXT_FONT,
                font_size="20px",
                font_weight="bold",
                text_align="left",
                padding="16px",
            ),
            width="100%",
            bg=PAGE_COLOR["azul_brasil"],
        ),
        
        # --- HEADER 2: Basic Info Grid ---
        rx.box(
            rx.grid(
                # Left Column
                rx.vstack(
                    rx.hstack(
                        rx.text("Código IBGE:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(cd_mun, color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    rx.hstack(
                        rx.text("UTP:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(utp_id, color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    rx.hstack(
                        rx.text("RM:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(regiao_metropolitana[:30] if len(str(regiao_metropolitana)) > 30 else regiao_metropolitana, 
                               color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    align_items="start",
                    spacing="2",
                ),
                
                # Right Column
                rx.vstack(
                    rx.hstack(
                        rx.text("REGIC:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(regic, color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    rx.hstack(
                        rx.text("População:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(pop_fmt, color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    rx.hstack(
                        rx.text("Total Viagens:", color=TEXT_COLOR["branco"], font_family=TEXT_FONT, font_size="11px"),
                        rx.text(viagens_fmt, color=TEXT_COLOR["amarelo_brasil"], font_family=TEXT_FONT, font_size="11px", font_weight="bold"),
                        spacing="1",
                    ),
                    align_items="start",
                    spacing="2",
                ),
                
                columns="2",
                spacing="4",
                width="100%",
                padding="12px 16px",
            ),
            width="100%",
            bg=PAGE_COLOR["azul_bandeira"],
        ),
        
        # --- BODY: Top 5 Destinations ---
        rx.flex(
            rx.vstack(
                rx.text(
                    "Top 5 Destinos de Fluxo",
                    color=TEXT_COLOR["azul_brasil"],
                    font_family=TEXT_FONT,
                    font_size="16px",
                    font_weight="bold",
                    padding="16px",
                ),
                
                # Table Header
                rx.box(
                    rx.hstack(
                        rx.text("#", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", width="40px"),
                        rx.text("Município", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", flex="1"),
                        rx.text("Dados Ter.", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", width="120px"),
                        rx.text("Dados Soc.", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", width="100px"),
                        rx.text("Viagens", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", text_align="right", width="80px"),
                        rx.text("% / Tempo", color="#666", font_family=TEXT_FONT, font_size="11px", font_weight="bold", text_align="right", width="70px"),
                        spacing="3",
                        width="100%",
                        padding="8px 12px",
                    ),
                    width="100%",
                    bg="#f0f0f0",
                    border_bottom="2px solid #ddd",
                ),
                
                # Destination Rows or Empty Message
                rx.cond(
                    len(top_destinations) > 0,
                    rx.vstack(
                        *[
                            destination_row(
                                rank=i + 1,
                                dest_name=dest[1],
                                dest_uf=dest[2],
                                dest_cd=dest[0],
                                dest_utp=dest[3],
                                dest_rm=dest[4],
                                dest_pop=dest[5],
                                dest_regic=dest[6],
                                flow_count=dest[7],
                                percentage=dest[8],
                                tempo_horas=dest[9] if len(dest) > 9 else None
                            )
                            for i, dest in enumerate(top_destinations[:5])
                        ],
                        width="100%",
                        spacing="0",
                    ),
                    rx.box(
                        rx.text(
                            "Sem dados de fluxo disponíveis",
                            color="#888",
                            font_family=TEXT_FONT,
                            font_size="14px",
                            font_style="italic",
                            text_align="center",
                            padding="24px",
                        ),
                        width="100%",
                    ),
                ),
                
                width="100%",
                spacing="0",
                align_items="start",
            ),
            direction="column",
            width="100%",
            bg=PAGE_COLOR["branco"],
            flex="1",
            overflow_y="auto",
        ),
        
        direction="column",
        width="100%",
        max_width="700px",
        max_height="600px",
        border_radius="8px",
        box_shadow="0px 4px 20px rgba(0, 0, 0, 0.15)",
        overflow="hidden",
    )
