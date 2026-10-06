# tests/test_dataform_workflow.py
"""dataform.yml routes each event to the right Dataform tag."""

from pathlib import Path

import yaml

WF = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/dataform.yml").read_text(encoding="utf-8"))
ON = WF[True] if True in WF else WF["on"]  # PyYAML parses the key `on` as True
TEXT = (Path(__file__).resolve().parents[1] / ".github/workflows/dataform.yml").read_text(encoding="utf-8")


def test_ml_complete_is_accepted():
    assert "ml_complete" in ON["repository_dispatch"]["types"]


def test_manual_runs_choose_a_piece():
    piece = ON["workflow_dispatch"]["inputs"]["piece"]
    assert piece["default"] == "full"
    assert piece["options"] == ["full", "core", "publish"]


def test_invocation_passes_included_tags():
    assert '"includedTags"' in TEXT
    assert "steps.piece.outputs.tag" in TEXT


def test_catalog_refresh_follows_ml_complete():
    assert "github.event.action == 'ml_complete'" in TEXT
