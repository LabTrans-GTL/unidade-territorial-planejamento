
import sys
import os
from pathlib import Path

# Add project root to sys.path to mimic app runtime
# We are currently in .../geovalida/frontend/
# We need to add .../geovalida/frontend to sys.path to import frontend.app
# We need to add .../geovalida to sys.path to import src (handled by state.py but good to double check)

current_dir = Path.cwd()
print(f"Current Working Directory: {current_dir}")

# Ensure we can import frontend module
sys.path.append(str(current_dir))

try:
    print("Attempting to import MapState...")
    from frontend.state import MapState
    print("MapState imported successfully.")
    
    print("Attempting to instantiate MapState...")
    state = MapState()
    print("MapState instantiated.")
    
    print("Attempting to generate map_html (this triggers data loading)...")
    # map_html is a property/var in Reflex, access it on the class or instance?
    # In Reflex, vars are attributes of the class.
    # But logic inside runs when accessed?
    # Actually, Reflex vars are descriptors. We might need to manually call the getter if available or just check the function.
    
    # Check the underlying logic function
    # The decorated function is usually available as _var_name or similar, or we just call the method if it was defined as a method.
    # In my code:
    # @rx.var
    # def map_html(self) -> str: ...
    
    # We can invoke it directly on an instance for testing if it's a normal method wrapper.
    html = state.map_html
    print(f"Map HTML generated. Length: {len(html)}")
    print(f"Map HTML Preview: {html[:100]}...")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
