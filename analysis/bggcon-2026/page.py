"""Where the flat files live."""
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA = HERE.parents[1] / "data" / "bggcon-2026"  # gitignored: source files in, built pages out
