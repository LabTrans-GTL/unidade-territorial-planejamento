import reflex as rx
from ..styles import PAGE_COLOR

# Colors
GREEN = "#009E2D"
YELLOW = PAGE_COLOR["amarelo_brasil"]
BLUE_DARK = PAGE_COLOR["azul_brasil"]
BLUE_LIGHT = PAGE_COLOR["azul_bandeira"]
WHITE = "white"

def tile(child: rx.Component, bg: str = WHITE) -> rx.Component:
    return rx.box(
        child,
        width="100%",
        padding_bottom="100%", # Aspect ratio 1:1 specifically for the square
        position="relative",
        bg=bg,
        overflow="hidden",
    )

def shape_container(child: rx.Component) -> rx.Component:
    return rx.box(
        child,
        position="absolute",
        top="0",
        left="0",
        width="100%",
        height="100%",
        display="flex",
        align_items="center",
        justify_content="center",
    )

# --- Shapes ---

def leaf(color: str) -> rx.Component:
    # Folha: cantos opostos arredondados
    return shape_container(
        rx.box(
            width="80%",
            height="80%",
            bg=color,
            border_radius="0 100% 0 100%",
        )
    )

def circle(color: str, size: str = "75%") -> rx.Component:
    return shape_container(
        rx.box(
            width=size,
            height=size,
            bg=color,
            border_radius="50%",
        )
    )

def diamond_outline(color: str, bg_inner: str = "transparent") -> rx.Component:
    # Losango vazado (borda grossa)
    return shape_container(
        rx.box(
            width="55%",
            height="55%",
            bg=bg_inner,
            border=f"8px solid {color}",
            style={"transform": "rotate(45deg)"},
        )
    )

def diamond_filled(color: str) -> rx.Component:
    # Losango cheio
    return shape_container(
        rx.box(
            width="60%",
            height="60%",
            bg=color,
            style={"transform": "rotate(45deg)"},
        )
    )

def triangle_up(color: str) -> rx.Component:
    return shape_container(
        rx.box(
            width="100%",
            height="100%",
            bg=color,
            style={
                "clip-path": "polygon(50% 15%, 15% 85%, 85% 85%)"
            }
        )
    )

def multiple_triangles() -> rx.Component:
    # 3 Triangulos brancos em fundo verde (como na imagem)
    tri = rx.box(
        bg="white", 
        width="33%", 
        height="100%", 
        style={"clip-path": "polygon(50% 20%, 0% 100%, 100% 100%)"}
    )
    return shape_container(
        rx.hstack(
            tri, tri, tri,
            width="100%",
            height="70%",
            justify="center",
            spacing="1",
            align_items="end", 
            padding_bottom="10%"
        )
    )

def four_circles(color: str) -> rx.Component:
    return shape_container(
        rx.grid(
            rx.box(bg=color, width="100%", height="100%", border_radius="50%"),
            rx.box(bg=color, width="100%", height="100%", border_radius="50%"),
            rx.box(bg=color, width="100%", height="100%", border_radius="50%"),
            rx.box(bg=color, width="100%", height="100%", border_radius="50%"),
            columns="2",
            gap="2",
            width="65%",
            height="65%",
        )
    )

def four_stars(color: str) -> rx.Component:
    star = rx.box(
        bg=color, 
        width="100%", 
        height="100%", 
        style={"clip-path": "polygon(50% 0%, 61% 35%, 100% 50%, 61% 65%, 50% 100%, 39% 65%, 0% 50%, 39% 35%)"}
    )
    return shape_container(
        rx.grid(
            star, star, star, star,
            columns="2",
            gap="2",
            width="70%",
            height="70%",
        )
    )

def large_star(color: str) -> rx.Component:
    return shape_container(
         rx.box(
            bg=color, 
            width="80%", 
            height="80%", 
            style={"clip-path": "polygon(50% 0%, 65% 35%, 100% 50%, 65% 65%, 50% 100%, 35% 65%, 0% 50%, 35% 35%)"}
        )
    )
    
def half_circles_flower(color: str) -> rx.Component:
    # Flor feita de 4 semi-círculos
    return shape_container(
        rx.box(
            # Top Left
            rx.box(bg=color, width="50%", height="50%", border_radius="0 100% 0 100%", position="absolute", top="0", left="0", transform="rotate(-45deg)"),
            # Top Right
            rx.box(bg=color, width="50%", height="50%", border_radius="0 100% 0 100%", position="absolute", top="0", right="0", transform="rotate(45deg)"),
             # Bottom Left
            rx.box(bg=color, width="50%", height="50%", border_radius="0 100% 0 100%", position="absolute", bottom="0", left="0", transform="rotate(225deg)"),
            # Bottom Right
            rx.box(bg=color, width="50%", height="50%", border_radius="0 100% 0 100%", position="absolute", bottom="0", right="0", transform="rotate(135deg)"),
            
            width="80%",
            height="80%",
            position="relative"
        )
    )


def login_background() -> rx.Component:
    return rx.center(
        rx.grid(
            # --- ROW 1 (7 tiles) ---
            # BGs: GREEN, WHITE, BLUE_DARK, YELLOW, GREEN, WHITE, BLUE_DARK
            tile(leaf(YELLOW), bg=GREEN),
            tile(diamond_outline(YELLOW), bg=WHITE),
            tile(circle(GREEN, "70%"), bg=BLUE_DARK),
            tile(leaf(GREEN), bg=YELLOW),
            tile(diamond_outline(YELLOW), bg=GREEN),
            tile(triangle_up(GREEN), bg=WHITE),
            tile(circle(YELLOW, "65%"), bg=BLUE_DARK),
            
            # --- ROW 2 (7 tiles) ---
            # BGs: YELLOW, BLUE_DARK, WHITE, GREEN, BLUE_DARK, YELLOW, GREEN
            tile(four_circles(BLUE_DARK), bg=YELLOW),
            tile(circle(GREEN, "65%"), bg=BLUE_DARK),
            tile(triangle_up(BLUE_DARK), bg=WHITE),
            tile(four_circles(BLUE_DARK), bg=GREEN),
            tile(four_circles(YELLOW), bg=BLUE_DARK),
            tile(circle(GREEN, "65%"), bg=YELLOW),
            tile(diamond_filled(WHITE), bg=GREEN),

            # --- ROW 3 (7 tiles) ---
            # BGs: GREEN, WHITE, BLUE_DARK, YELLOW, GREEN, WHITE, BLUE_DARK
            tile(multiple_triangles(), bg=GREEN),
            tile(four_stars(YELLOW), bg=WHITE), 
            tile(leaf(YELLOW), bg=BLUE_DARK),
            tile(large_star(WHITE), bg=YELLOW),
            tile(diamond_filled(BLUE_DARK), bg=GREEN),
            tile(four_stars(YELLOW), bg=WHITE),
            tile(leaf(WHITE), bg=BLUE_DARK),

            # --- ROW 4 (7 tiles) ---
            # BGs: WHITE, GREEN, YELLOW, BLUE_DARK, WHITE, YELLOW, GREEN
            tile(diamond_filled(BLUE_DARK), bg=WHITE), 
            tile(large_star(YELLOW), bg=GREEN),
            tile(half_circles_flower(BLUE_DARK), bg=YELLOW),
            tile(diamond_filled(GREEN), bg=BLUE_DARK), 
            tile(large_star(YELLOW), bg=WHITE),
            tile(leaf(GREEN), bg=YELLOW),
            tile(four_circles(BLUE_DARK), bg=GREEN),

            width="100%",
            # Ensure grid is always wide enough to generate enough height (ratio 7:4 => width needs to be 1.75x height)
            min_width="175vh", 
            columns="7",
            gap="0", # NO GAP
        ),
        width="100%",
        height="100%",
        bg=WHITE,
        overflow="hidden",
        opacity="0.9", # Slightly transparent
    )
