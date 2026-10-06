# tests/test_deploy_warehouse_api_paths.py
"""Every src module the warehouse API imports is in its deploy workflow's path filter.

The API image copies the whole repo, so a change to any module it imports changes what
runs in production. #147 changed src/monitoring/chain.py, which was not in the filter,
so the API kept serving the old chain until a manual deploy.
"""

import ast
from fnmatch import fnmatch
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/deploy-warehouse-api.yml"


def _paths() -> list[str]:
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]  # PyYAML parses the key `on` as True
    return on["push"]["paths"]


def _module_file(name: str) -> Path | None:
    base = ROOT / Path(*name.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.exists():
            return candidate
    return None


def _src_imports(path: Path) -> set[Path]:
    found = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        names = []
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module] + [f"{node.module}.{a.name}" for a in node.names]
        elif isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        for name in names:
            if name.split(".")[0] in ("src", "services"):
                module = _module_file(name)
                if module is not None:
                    found.add(module)
    return found


def _api_modules() -> set[Path]:
    seen: set[Path] = set()
    todo = list((ROOT / "services/warehouse_api").rglob("*.py"))
    while todo:
        path = todo.pop()
        if path in seen:
            continue
        seen.add(path)
        todo.extend(_src_imports(path) - seen)
    return seen


def test_every_imported_module_triggers_a_deploy():
    patterns = _paths()
    uncovered = sorted(
        p.relative_to(ROOT).as_posix()
        for p in _api_modules()
        if not any(fnmatch(p.relative_to(ROOT).as_posix(), pat) for pat in patterns)
    )
    assert uncovered == []


def test_workflow_redeploys_when_it_changes():
    assert ".github/workflows/deploy-warehouse-api.yml" in _paths()
