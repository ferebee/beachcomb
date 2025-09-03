import os
import re
import sys
import subprocess
from pathlib import Path

# Make sure the source package is importable without installing
ROOT = Path(__file__).resolve().parents[1]
ENV = os.environ.copy()
ENV["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + ENV.get("PYTHONPATH", "")


def run_module(args):
    """Run `python -m beachcomb <args>` and return CompletedProcess."""
    return subprocess.run(
        [sys.executable, "-m", "beachcomb", *args],
        env=ENV,
        capture_output=True,
        text=True,
    )


def test_package_imports():
    # Import beachcomb from src/ directly
    sys.path.insert(0, str(ROOT / "src"))
    import importlib

    bc = importlib.import_module("beachcomb")
    assert hasattr(bc, "__version__")
    assert isinstance(bc.__version__, str)
    assert re.match(r"^\d+\.\d+\.\d+$", bc.__version__)


def test_cli_help_exits_zero():
    cp = run_module(["--help"])
    # argparse may write help to stdout or stderr, so we check both
    combined = (cp.stdout + cp.stderr).lower()
    assert cp.returncode == 0
    assert "usage" in combined or "help" in combined


def test_cli_version_exits_zero_and_prints_version():
    cp = run_module(["--version"])
    combined = cp.stdout + cp.stderr
    assert cp.returncode == 0
    # Look for something like 0.1.0 in the output
    assert re.search(r"\b\d+\.\d+\.\d+\b", combined)

def test_cli_dry_run_on_empty_dirs(tmp_path):
    (tmp_path / "src--testdir").mkdir()
    (tmp_path / "dest--testdir").mkdir()
    cp = run_module(["--source", str(tmp_path / "src--testdir"),
                     "--dest", str(tmp_path / "dest--testdir"),
                     "--dry-run"])
    # If your CLI prints warnings to stderr, don't require clean stderr—just require success
    assert cp.returncode == 0
