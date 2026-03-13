from __future__ import annotations

import argparse
import ast
import json
import os
import shutil
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple


@dataclass(frozen=True)
class ImportRef:
    src: str
    dst: str
    kind: str


@dataclass(frozen=True)
class FileRef:
    src: str
    path: str
    kind: str


def _is_truthy_env(v: Optional[str]) -> bool:
    return (v or "").strip() in ("1", "true", "TRUE", "yes", "YES", "on", "ON")


def _safe_relpath(p: Path, root: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except Exception:
        return p.as_posix()


def _is_python_file(p: Path) -> bool:
    return p.is_file() and p.suffix.lower() == ".py"


def _read_text_best_effort(p: Path) -> Optional[str]:
    try:
        return p.read_text(encoding="utf-8")
    except Exception:
        try:
            return p.read_text(encoding="utf-8-sig")
        except Exception:
            return None


def _parse_ast(p: Path) -> Optional[ast.AST]:
    src = _read_text_best_effort(p)
    if src is None:
        return None
    try:
        return ast.parse(src, filename=str(p))
    except Exception:
        return None


def _has_main_guard(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not isinstance(test, ast.Compare):
            continue
        if not (isinstance(test.left, ast.Name) and test.left.id == "__name__"):
            continue
        if len(test.ops) != 1 or not isinstance(test.ops[0], ast.Eq):
            continue
        if len(test.comparators) != 1:
            continue
        rhs = test.comparators[0]
        if isinstance(rhs, ast.Constant) and rhs.value == "__main__":
            return True
    return False


def _module_name_for_path(py_path: Path, root: Path) -> Optional[str]:
    try:
        rel = py_path.relative_to(root)
    except Exception:
        return None
    if rel.suffix.lower() != ".py":
        return None
    parts = list(rel.parts)
    if not parts:
        return None
    parts[-1] = parts[-1][:-3]
    if parts[-1] == "__init__":
        parts = parts[:-1]
    if not parts:
        return None
    return ".".join(parts)


def _build_module_index(root: Path, all_py: Sequence[Path]) -> Dict[str, Path]:
    idx: Dict[str, Path] = {}
    for p in all_py:
        m = _module_name_for_path(p, root)
        if m:
            idx[m] = p
    return idx


def _resolve_import(
    *,
    importer_path: Path,
    importer_mod: str,
    node: ast.AST,
    module_index: Dict[str, Path],
) -> List[str]:
    out: List[str] = []
    if isinstance(node, ast.Import):
        for a in node.names:
            name = str(a.name)
            if name in module_index:
                out.append(name)
                continue
            prefix = name
            while "." in prefix:
                prefix = prefix.rsplit(".", 1)[0]
                if prefix in module_index:
                    out.append(prefix)
                    break
        return out

    if not isinstance(node, ast.ImportFrom):
        return out

    level = int(node.level or 0)
    mod = node.module
    base = str(mod) if isinstance(mod, str) and mod else ""

    if level > 0:
        importer_parts = importer_mod.split(".")
        if importer_path.name == "__init__.py":
            pkg_parts = importer_parts
        else:
            pkg_parts = importer_parts[:-1]
        if level > len(pkg_parts):
            return out
        prefix_parts = pkg_parts[: len(pkg_parts) - level + 1]
        if base:
            full_base = ".".join(prefix_parts + base.split("."))
        else:
            full_base = ".".join(prefix_parts)
    else:
        full_base = base

    if full_base in module_index:
        out.append(full_base)

    for a in node.names:
        if a.name == "*":
            continue
        cand = (full_base + "." + a.name).strip(".")
        if cand in module_index:
            out.append(cand)
            continue
        if full_base in module_index:
            out.append(full_base)
    return out


def _const_str(node: ast.AST) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_file_refs(tree: ast.AST, *, src_mod: str) -> List[FileRef]:
    refs: List[FileRef] = []

    def add(path: str, kind: str) -> None:
        if not path or not isinstance(path, str):
            return
        refs.append(FileRef(src=src_mod, path=path, kind=kind))

    for n in ast.walk(tree):
        if isinstance(n, ast.Call):
            fn = n.func
            if isinstance(fn, ast.Name) and fn.id == "open" and n.args:
                s = _const_str(n.args[0])
                if s:
                    add(s, "open")
            if isinstance(fn, ast.Attribute) and fn.attr == "open" and n.args:
                s = _const_str(n.args[0])
                if s:
                    add(s, "path_open")

            if isinstance(fn, ast.Attribute) and fn.attr in ("read_text", "read_bytes") and n.args == []:
                if isinstance(fn.value, ast.Call) and isinstance(fn.value.func, ast.Name) and fn.value.func.id in ("Path",):
                    if fn.value.args:
                        s = _const_str(fn.value.args[0])
                        if s:
                            add(s, f"Path.{fn.attr}")

            if isinstance(fn, ast.Attribute) and fn.attr in ("read_csv", "read_parquet", "read_json") and n.args:
                s = _const_str(n.args[0])
                if s:
                    add(s, f"pd.{fn.attr}")
    return refs


def _extract_dynamic_imports(tree: ast.AST) -> Tuple[Set[str], bool]:
    mods: Set[str] = set()
    unknown = False

    def mark_unknown() -> None:
        nonlocal unknown
        unknown = True

    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        fn = n.func
        name: Optional[str] = None
        if isinstance(fn, ast.Name):
            name = fn.id
        elif isinstance(fn, ast.Attribute):
            name = fn.attr

        if name in ("__import__", "import_module"):
            if not n.args:
                mark_unknown()
                continue
            s = _const_str(n.args[0])
            if s:
                mods.add(s)
            else:
                mark_unknown()
    return mods, unknown


def _default_excludes(root: Path) -> Set[str]:
    return {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        "logs",
        "_deprecated_archive",
    }


def _is_under_any(p: Path, roots: Sequence[Path]) -> bool:
    for r in roots:
        try:
            p.relative_to(r)
            return True
        except Exception:
            continue
    return False


def _collect_files(root: Path, *, exclude_dirs: Set[str]) -> Tuple[List[Path], List[Path]]:
    all_files: List[Path] = []
    all_py: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        d = Path(dirpath)
        dirnames[:] = [x for x in dirnames if x not in exclude_dirs and not x.startswith(".mypy_cache")]
        for fn in filenames:
            p = d / fn
            if p.is_file():
                all_files.append(p)
                if _is_python_file(p):
                    all_py.append(p)
    return all_files, all_py


def _find_entrypoints(root: Path, all_py: Sequence[Path], module_index: Dict[str, Path]) -> Set[str]:
    explicit = {
        "engine.py",
        "api_server.py",
        "mentor_executor.py",
        "mt5_executor.py",
        "commissioning_runner.py",
        "intelligent_dashboard.py",
        "intelligent_mentor_readonly.py",
        "ai_llm_confirm_smoke_test.py",
        "watchdog_supervisor.py",
    }
    entry_mods: Set[str] = set()
    for p in all_py:
        if p.name in explicit:
            m = _module_name_for_path(p, root)
            if m:
                entry_mods.add(m)
                continue
    for m in ("tests.conftest",):
        if m in module_index:
            entry_mods.add(m)
    return entry_mods


def _find_entrypoints_all(root: Path, all_py: Sequence[Path], module_index: Dict[str, Path]) -> Set[str]:
    entry_mods = _find_entrypoints(root, all_py, module_index)
    for p in all_py:
        t = _parse_ast(p)
        if t is None:
            continue
        if _has_main_guard(t):
            m = _module_name_for_path(p, root)
            if m:
                entry_mods.add(m)
    return entry_mods


def _resolve_file_ref_to_repo_path(root: Path, raw: str) -> Optional[Path]:
    s = raw.strip()
    if not s:
        return None
    s = s.replace("\\", "/")
    if "://" in s:
        return None
    p = Path(s)
    if p.is_absolute():
        try:
            rp = p.resolve()
            if _is_under_any(rp, [root.resolve()]):
                return rp
        except Exception:
            return None
        return None
    cand = (root / p).resolve()
    if cand.exists() and _is_under_any(cand, [root.resolve()]):
        return cand
    return None


def analyze(
    *,
    root: Path,
    include_tests: bool,
    include_docs: bool,
    profile: str,
) -> Dict[str, Any]:
    exclude_dirs = _default_excludes(root)
    all_files, all_py = _collect_files(root, exclude_dirs=exclude_dirs)

    module_index = _build_module_index(root, all_py)
    entry_mods = _find_entrypoints_all(root, all_py, module_index) if profile == "all" else _find_entrypoints(root, all_py, module_index)

    imports: List[ImportRef] = []
    file_refs: List[FileRef] = []
    dynamic_unknown_mods: Set[str] = set()

    parsed_cache: Dict[str, Optional[ast.AST]] = {}

    def get_tree(mod: str) -> Optional[ast.AST]:
        if mod in parsed_cache:
            return parsed_cache[mod]
        p = module_index.get(mod)
        if p is None:
            parsed_cache[mod] = None
            return None
        t = _parse_ast(p)
        parsed_cache[mod] = t
        return t

    for mod, p in module_index.items():
        t = get_tree(mod)
        if t is None:
            continue
        for node in ast.walk(t):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for dst in _resolve_import(importer_path=p, importer_mod=mod, node=node, module_index=module_index):
                    imports.append(ImportRef(src=mod, dst=dst, kind=type(node).__name__))
        file_refs.extend(_extract_file_refs(t, src_mod=mod))
        dyn_mods, unknown = _extract_dynamic_imports(t)
        for dm in dyn_mods:
            if dm in module_index:
                imports.append(ImportRef(src=mod, dst=dm, kind="dynamic_import"))
        if unknown:
            dynamic_unknown_mods.add(mod)

    adjacency: Dict[str, Set[str]] = defaultdict(set)
    incoming: Counter[str] = Counter()
    for r in imports:
        adjacency[r.src].add(r.dst)
        incoming[r.dst] += 1

    reachable: Set[str] = set()
    q: deque[str] = deque(entry_mods)
    while q:
        m = q.popleft()
        if m in reachable:
            continue
        reachable.add(m)
        for nxt in adjacency.get(m, set()):
            if nxt not in reachable:
                q.append(nxt)

    used_paths: Set[Path] = set()
    for m in reachable:
        p = module_index.get(m)
        if p is not None:
            used_paths.add(p)

    file_ref_incoming: Counter[str] = Counter()
    used_asset_paths: Set[Path] = set()
    for fr in file_refs:
        rp = _resolve_file_ref_to_repo_path(root, fr.path)
        if rp is None:
            continue
        file_ref_incoming[_safe_relpath(rp, root)] += 1
        used_asset_paths.add(rp)

    used_paths |= used_asset_paths

    excluded_files = {
        ".env",
        ".gitignore",
        "config.json",
        "pyproject.toml",
        "ruff.toml",
        "requirements.txt",
        "requirements-dev.txt",
        "README.md",
        "README.txt",
        "orphan_report.json",
    }

    def is_skippable(p: Path) -> bool:
        rel = _safe_relpath(p, root)
        if p.name.startswith(".") and p.suffix.lower() in (".json", ".jsonl", ".log", ".csv"):
            return True
        if p.name in excluded_files:
            return True
        if rel.startswith("tests/") and not include_tests:
            return True
        if rel.startswith("docs/") and not include_docs:
            return True
        if rel.startswith("stateDiagram-") and not include_docs:
            return True
        return False

    orphaned: List[Dict[str, Any]] = []
    for p in all_files:
        rel = _safe_relpath(p, root)
        if is_skippable(p):
            continue
        if p in used_paths:
            continue
        reasons: List[str] = []
        if _is_python_file(p):
            mod = _module_name_for_path(p, root) or rel
            if mod not in reachable:
                reasons.append("unreachable_from_entrypoints")
            if incoming.get(mod, 0) == 0 and mod not in entry_mods:
                reasons.append("0_incoming_imports")
            if dynamic_unknown_mods:
                reasons.append("dynamic_imports_present_in_project")
            orphaned.append(
                {
                    "path": rel,
                    "kind": "python",
                    "incoming_imports": int(incoming.get(mod, 0)),
                    "flag_reasons": reasons,
                }
            )
        else:
            reasons.append("unreferenced_asset")
            orphaned.append(
                {
                    "path": rel,
                    "kind": "asset",
                    "incoming_references": int(file_ref_incoming.get(rel, 0)),
                    "flag_reasons": reasons,
                }
            )

    return {
        "root": root.as_posix(),
        "profile": profile,
        "entry_modules": sorted(entry_mods),
        "reachable_modules_count": int(len(reachable)),
        "python_files_count": int(len(all_py)),
        "all_files_count": int(len(all_files)),
        "dynamic_import_unknown_modules": sorted(dynamic_unknown_mods),
        "orphaned": orphaned,
    }


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def archive(
    *,
    root: Path,
    archive_dir: Path,
    orphaned_relpaths: Sequence[str],
    dry_run: bool,
) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    for rel in orphaned_relpaths:
        src = (root / Path(rel)).resolve()
        if not src.exists() or not src.is_file():
            continue
        dst = (archive_dir / Path(rel)).resolve()
        actions.append({"src": _safe_relpath(src, root), "dst": _safe_relpath(dst, root), "action": "move" if not dry_run else "dry_run"})
        if dry_run:
            continue
        _ensure_parent_dir(dst)
        shutil.move(str(src), str(dst))
    return actions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--archive-dir", default="_deprecated_archive")
    ap.add_argument("--report", default="orphan_report.json")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--include-tests", action="store_true")
    ap.add_argument("--include-docs", action="store_true")
    ap.add_argument("--profile", choices=["production", "all"], default="production")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    archive_dir = (root / args.archive_dir).resolve()

    report = analyze(root=root, include_tests=args.include_tests, include_docs=args.include_docs, profile=str(args.profile))
    orphans = report.get("orphaned", [])
    orphan_paths = [str(x.get("path")) for x in orphans if isinstance(x, dict) and isinstance(x.get("path"), str)]

    actions = archive(root=root, archive_dir=archive_dir, orphaned_relpaths=orphan_paths, dry_run=bool(args.dry_run))
    report["archive_dir"] = _safe_relpath(archive_dir, root)
    report["dry_run"] = bool(args.dry_run)
    report["actions"] = actions

    out_path = (root / args.report).resolve()
    _ensure_parent_dir(out_path)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
