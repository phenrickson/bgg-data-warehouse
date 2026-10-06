# tests/test_release_please.py
"""release-please owns versioning: config shape, workflow, and versions that agree."""

import json
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "release-please-config.json"
MANIFEST = ROOT / ".release-please-manifest.json"
WORKFLOW = ROOT / ".github/workflows/release-please.yml"


def _package() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))["packages"]["."]


def test_config():
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    pkg = config["packages"]["."]
    assert pkg["release-type"] == "python"
    assert pkg["package-name"] == "bgg-data-warehouse"
    assert pkg["include-component-in-tag"] is False
    # v0.7.0 is a GitHub Release, so release-please finds the last release itself.
    assert "last-release-sha" not in config


def test_uv_lock_is_bumped_by_the_toml_updater():
    lock = [f for f in _package()["extra-files"] if f["path"] == "uv.lock"]
    assert lock == [{
        "type": "toml",
        "path": "uv.lock",
        "jsonpath": "$.package[?(@.name.value=='bgg-data-warehouse')].version",
    }]


def test_changelog_sections():
    sections = {s["type"]: s for s in _package()["changelog-sections"]}
    assert sections["ci"]["section"] == "Operations"
    assert sections["ci"].get("hidden", False) is False
    assert sections["docs"]["hidden"] is True
    for visible in ("feat", "fix", "perf", "revert"):
        assert sections[visible].get("hidden", False) is False


def test_versions_agree():
    """pyproject, uv.lock and the manifest name the same version (0.6.6 bumped only one)."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = [p["version"] for p in lock["package"] if p["name"] == project["name"]]
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))["."]
    assert locked == [project["version"]]
    assert manifest == project["version"]


def test_workflow():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]  # PyYAML parses the key `on` as True
    assert on == {"push": {"branches": ["main"]}}
    assert wf["permissions"] == {"contents": "write", "pull-requests": "write"}
    step = wf["jobs"]["release-please"]["steps"][0]
    assert step["uses"] == "googleapis/release-please-action@v4"
    assert step["with"]["config-file"] == "release-please-config.json"
    assert step["with"]["manifest-file"] == ".release-please-manifest.json"


def test_tag_release_retired():
    assert not (ROOT / ".github/workflows/tag-release.yml").exists()
