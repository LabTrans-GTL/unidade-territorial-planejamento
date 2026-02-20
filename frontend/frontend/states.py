import reflex as rx

class SidebarState(rx.State):
    """Estado para controlar a sidebar."""
    show_sidebar: bool = True

    def toggle_sidebar(self):
        self.show_sidebar = not self.show_sidebar
