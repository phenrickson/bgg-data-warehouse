"""The lineage GitHub action still writes both diagrams, now via the shared parser."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests/fixtures/dataform_compilation_2026-10-03.json"


def _load_action():
    path = ROOT / ".github/actions/dataform-lineage/generate_lineage.py"
    spec = importlib.util.spec_from_file_location("generate_lineage", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_action_uses_the_shared_parser():
    action = _load_action()
    from src.monitoring import lineage
    assert action._parse is lineage.parse_compilation_result


def test_action_writes_mermaid_and_visjs(tmp_path, monkeypatch):
    action = _load_action()
    monkeypatch.setenv("COMPILATION_DETAILS", FIX.read_text())
    monkeypatch.setenv("OUTPUT_FORMATS", "mermaid,visjs")
    monkeypatch.setenv("OUTPUT_DIR", str(tmp_path))
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    action.main()
    md = (tmp_path / "lineage.md").read_text()
    assert "graph LR" in md
    assert ('bgg_data_warehouse_analytics_games_active["analytics.games_active"] --> '
            'bgg_data_warehouse_analytics_games_features["analytics.games_features"]') in md
    assert 'bgg_predictive_models_raw_ml_predictions_landing["raw.ml_predictions_landing"]' in md
    assert "vis.DataSet" in (tmp_path / "lineage.html").read_text()
