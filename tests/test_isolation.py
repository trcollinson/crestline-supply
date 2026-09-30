"""Truth isolation: data-platform code never reads the answer key.

The rule: code in pipelines/, interfaces/, queries/, and infra/
(and the Python that runs it) must never read out/truth/ or import the
simulator. Only eval/ and the tests may. This checks the rule mechanically:
no SQL mentions truth outside a comment, and no Python imports `crestline.sim`
or has "truth" in a string that isn't a docstring.
"""

import ast
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PLATFORM_DIRS = ["pipelines", "interfaces", "queries", "infra"]
PLATFORM_MODULES = [
    "src/crestline/query.py",
    "src/crestline/mess.py",
    "src/crestline/upload.py",
    "src/crestline/stack.py",
    "src/crestline/athena.py",
]
SQL_COMMENT = re.compile(r"--[^\n]*")


def platform_files() -> list[Path]:
    files = [REPO_ROOT / module for module in PLATFORM_MODULES]
    for folder in PLATFORM_DIRS:
        files += [
            path
            for path in (REPO_ROOT / folder).rglob("*")
            if path.suffix in {".py", ".sql"} and "cdk.out" not in path.parts
        ]
    return files


def non_docstring_strings(tree: ast.AST) -> list[str]:
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and id(node) not in docstrings
    ]


def imported_modules(tree: ast.AST) -> list[str]:
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def test_there_is_platform_code_to_check():
    assert any(path.suffix == ".sql" for path in platform_files())


def test_platform_sql_never_mentions_the_answer_key():
    for path in platform_files():
        if path.suffix == ".sql":
            code = SQL_COMMENT.sub("", path.read_text(encoding="utf-8"))
            assert "truth" not in code.lower(), path


def test_platform_python_never_imports_the_simulator_or_reads_truth():
    for path in platform_files():
        if path.suffix == ".py":
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for module in imported_modules(tree):
                assert not module.startswith("crestline.sim"), (path, module)
            for text in non_docstring_strings(tree):
                assert "truth" not in text.lower(), (path, text)
