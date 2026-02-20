"""Basic import and path tests."""
import sys
from pathlib import Path

# Ensure project root is on path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def test_project_structure():
    """Verify expected directories exist."""
    assert (PROJECT_ROOT / "src").exists()
    assert (PROJECT_ROOT / "data").exists()
    assert (PROJECT_ROOT / "config").exists()
    assert (PROJECT_ROOT / "output").exists()
    assert (PROJECT_ROOT / "docs").exists()
    assert (PROJECT_ROOT / "assets").exists()


def test_src_imports():
    """Verify core modules can be imported."""
    from src.data import build_master_tables
    from src.features import build_targets
    assert build_master_tables is not None
    assert build_targets is not None
