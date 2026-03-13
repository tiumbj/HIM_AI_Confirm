from __future__ import annotations

import argparse
import ast
import os
import shutil
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple


WHITELIST_BASENAMES = {
    "__init__.py",
    "models.py",
    "admin.py",
    "tasks.py",
    "conftest.py",
    "setup.py",
}


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    kind: str


def _safe_relpath(p: Path, root: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except Exception:
        return p.as_posix()


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


def _is_python_like(p: Path) -> bool:
    return p.is_file() and p.suffix.lower() in (".py", ".txt")


def _is_python_module(p: Path) -> bool:
    return p.is_file() and p.suffix.lower() == ".py"


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


def _resolve_import_from(
    *,
    importer_path: Path,
    importer_mod: str,
    node: ast.ImportFrom,
    module_index: Dict[str, Path],
) -> List[str]:
    out: List[str] = []
    level = int(node.level or 0)
    mod = node.module
    base = str(mod) if isinstance(mod, str) and mod else ""

    if level > 0:
        importer_parts = importer_mod.split(".")
        pkg_parts = importer_parts if importer_path.name == "__init__.py" else importer_parts[:-1]
        if level > len(pkg_parts):
            return out
        prefix_parts = pkg_parts[: len(pkg_parts) - level + 1]
        full_base = ".".join(prefix_parts + (base.split(".") if base else []))
    else:
        full_base = base

    if full_base and full_base in module_index:
        out.append(full_base)

    for a in node.names:
        if a.name == "*":
            continue
        cand = (full_base + "." + a.name).strip(".")
        if cand in module_index:
            out.append(cand)
        elif full_base in module_index:
            out.append(full_base)
    return out


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
    if isinstance(node, ast.ImportFrom):
        return _resolve_import_from(importer_path=importer_path, importer_mod=importer_mod, node=node, module_index=module_index)
    return out


def _collect_files(root: Path, *, exclude_dirs: Set[str]) -> Tuple[List[Path], List[Path], List[Path]]:
    all_py_like: List[Path] = []
    all_py: List[Path] = []
    all_txt: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        d = Path(dirpath)
        dirnames[:] = [x for x in dirnames if x not in exclude_dirs]
        for fn in filenames:
            p = d / fn
            if not _is_python_like(p):
                continue
            all_py_like.append(p)
            if p.suffix.lower() == ".py":
                all_py.append(p)
            elif p.suffix.lower() == ".txt":
                all_txt.append(p)
    return all_py_like, all_py, all_txt


def _default_exclude_dirs() -> Set[str]:
    return {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "_PY_QUARANTINE_",
        "_QUARANTINE_BIN_",
        "_deprecated_archive",
        "logs",
    }


def analyze_orphan_modules(
    *,
    root: Path,
    entry_files: Sequence[str],
) -> Dict[str, object]:
    exclude_dirs = _default_exclude_dirs()
    all_py_like, all_py, all_txt = _collect_files(root, exclude_dirs=exclude_dirs)
    module_index = _build_module_index(root, all_py)

    entry_modules: List[str] = []
    entry_relpaths: Set[str] = set()
    for ef in entry_files:
        ep = (root / Path(ef)).resolve()
        if not ep.exists():
            raise FileNotFoundError(f"Entrypoint not found: {ef}")
        m = _module_name_for_path(ep, root)
        if not m:
            raise ValueError(f"Entrypoint must be a .py under root: {ef}")
        entry_modules.append(m)
        entry_relpaths.add(_safe_relpath(ep, root))

    edges: List[Edge] = []
    adjacency: Dict[str, Set[str]] = defaultdict(set)
    incoming: Counter[str] = Counter()

    for mod, p in module_index.items():
        t = _parse_ast(p)
        if t is None:
            continue
        for node in ast.walk(t):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for dst in _resolve_import(importer_path=p, importer_mod=mod, node=node, module_index=module_index):
                    edges.append(Edge(src=mod, dst=dst, kind=type(node).__name__))
                    adjacency[mod].add(dst)
                    incoming[dst] += 1

    for txt_path in all_txt:
        t = _parse_ast(txt_path)
        if t is None:
            continue
        src = f"TXT:{_safe_relpath(txt_path, root)}"
        for node in ast.walk(t):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                for dst in _resolve_import(importer_path=txt_path, importer_mod=src, node=node, module_index=module_index):
                    edges.append(Edge(src=src, dst=dst, kind=type(node).__name__))
                    incoming[dst] += 1

    reachable: Set[str] = set()
    q: deque[str] = deque(entry_modules)
    while q:
        m = q.popleft()
        if m in reachable:
            continue
        reachable.add(m)
        for nxt in adjacency.get(m, set()):
            if nxt not in reachable:
                q.append(nxt)

    orphaned: List[str] = []
    active: List[str] = []
    for mod, p in module_index.items():
        rel = _safe_relpath(p, root)
        if p.name in WHITELIST_BASENAMES:
            active.append(rel)
            continue
        if rel in entry_relpaths:
            active.append(rel)
            continue
        if p.name == "py_quarantine_orphans.py":
            active.append(rel)
            continue
        if mod in reachable:
            active.append(rel)
            continue
        orphaned.append(rel)

    orphaned = sorted(set(orphaned))
    active = sorted(set(active))

    return {
        "total_files_scanned": int(len(all_py_like)),
        "python_files_scanned": int(len(all_py)),
        "text_files_scanned": int(len(all_txt)),
        "entrypoints": list(entry_files),
        "entry_modules": entry_modules,
        "active_files": active,
        "orphaned_files": orphaned,
        "edges_count": int(len(edges)),
        "local_modules_count": int(len(module_index)),
        "incoming_imports_orphaned_0_count": int(sum(1 for rel in orphaned if incoming.get(_module_name_for_path((root / rel).resolve(), root) or "", 0) == 0)),
    }


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def quarantine_orphans(
    *,
    root: Path,
    quarantine_dir: Path,
    orphaned_relpaths: Sequence[str],
    dry_run: bool,
) -> List[str]:
    moved: List[str] = []
    for rel in orphaned_relpaths:
        src = (root / Path(rel)).resolve()
        if not src.exists() or not src.is_file():
            continue
        dst = (quarantine_dir / Path(rel)).resolve()
        if dry_run:
            moved.append(rel)
            continue
        _ensure_parent_dir(dst)
        shutil.move(str(src), str(dst))
        moved.append(rel)
    return moved


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--entry", action="append", default=[], help="Entrypoint file under root (repeatable)")
    ap.add_argument("--quarantine-dir", default="_PY_QUARANTINE_")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    entry_files = [str(x) for x in (args.entry or []) if str(x).strip()]
    if not entry_files:
        print("ERROR: Provide at least one --entry <file.py>", flush=True)
        return 2

    report = analyze_orphan_modules(root=root, entry_files=entry_files)

    quarantine_dir = (root / str(args.quarantine_dir)).resolve()
    orphaned = [str(x) for x in report.get("orphaned_files", []) if isinstance(x, str)]
    moved = quarantine_orphans(root=root, quarantine_dir=quarantine_dir, orphaned_relpaths=orphaned, dry_run=bool(args.dry_run))

    print("=== PY ORPHAN QUARANTINE ===")
    print(f"Root: {root.as_posix()}")
    print(f"Quarantine: {quarantine_dir.as_posix()}")
    print(f"Dry-run: {bool(args.dry_run)}")
    print(f"Total files scanned (.py+.txt): {report['total_files_scanned']}")
    print(f"Python files scanned: {report['python_files_scanned']}")
    print(f"Text files scanned: {report['text_files_scanned']}")
    print(f"Local modules indexed: {report['local_modules_count']}")
    print(f"Edges discovered: {report['edges_count']}")
    print(f"Active files: {len(report['active_files'])}")
    print(f"Orphaned files: {len(report['orphaned_files'])}")
    if moved:
        print("\nMoved to quarantine:")
        for p in moved:
            print(f"- {p}")
    else:
        print("\nNo orphaned files moved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
