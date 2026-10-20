"""Keep domain/application imports free of frameworks and concrete adapters."""

import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1] / "app"


@pytest.mark.parametrize("layer", ["domain", "application", "shared"])
def test_dependency_direction(layer):
    allowed = {
        "domain": {"domain", "shared"},
        "application": {"application", "domain", "shared"},
        "shared": {"shared"},
    }[layer]
    frameworks = {"sqlalchemy", "fastapi", "faststream", "httpx", "pydantic"}
    for path in (ROOT / layer).rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            modules = []
            if isinstance(node, ast.ImportFrom):
                assert not node.level, f"Use explicit imports for boundary checks: {path}"
                modules = [node.module or ""]
            elif isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            for module in modules:
                parts = module.split(".")
                assert parts[0] not in frameworks, (path, module)
                if parts[0] == "app":
                    assert len(parts) > 1 and parts[1] in allowed, (path, module)
