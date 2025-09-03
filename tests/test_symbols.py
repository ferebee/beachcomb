import sys
from pathlib import Path
import importlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

def test_can_import_core_symbols():
    # Adjust these to whatever public symbols you expect to exist
    bc = importlib.import_module("beachcomb")
    core = importlib.import_module("beachcomb.core")

    # If you re-export Planner in __init__.py, this asserts the shim works
    # If you didn't, just assert it's in core.
    assert hasattr(core, "Planner")
