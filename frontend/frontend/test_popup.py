import reflex as rx
from .components.popup_flow import popup_flow

def test_popup_flow():
    """Test page for the popup_flow component."""
    
    # Sample data - Florianópolis
    sample_destinations = [
        ("4209102", "São José", "SC", "4205400", "Grande Florianópolis", 254680, "Centro Sub-regional A", 12450, 25.3, 0.25),
        ("4202404", "Biguaçu", "SC", "4205400", "Grande Florianópolis", 69405, "Centro de Zona B", 8230, 16.7, 0.33),
        ("4205803", "Garopaba", "SC", "4205400", "-", 22193, "-", 6120, 12.4, 0.75),
        ("4204152", "Camboriú", "SC", "4204100", "Vale do Itajaí", 82257, "Centro de Zona A", 4890, 9.9, 1.25),
        ("4204608", "Criciúma", "SC", "4204600", "-", 217713, "Capital Regional C", 3210, 6.5, 1.8),
    ]
    
    return rx.center(
        rx.vstack(
            rx.text(
                "Exemplo: Popup de Fluxo",
                font_size="24px",
                font_weight="bold",
                margin_bottom="20px",
            ),
            
            popup_flow(
                nm_mun="Florianópolis",
                cd_mun="4205407",
                uf="SC",
                utp_id="4205400",
                regiao_metropolitana="Grande Florianópolis",
                regic="Capital Regional B",
                populacao=508826,
                total_viagens=49300,
                top_destinations=sample_destinations
            ),
            
            spacing="4",
            width="100%",
            max_width="800px",
        ),
        width="100%",
        min_height="100vh",
        bg="#f2f2f2",
        padding="40px",
    )
