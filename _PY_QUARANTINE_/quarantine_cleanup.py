from __future__ import annotations

import argparse
import json
import os
import shutil
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class QuarantineItem:
    path: str
    kind: str
    reasons: List[str]


def _safe_relpath(p: Path, root: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except Exception:
        return p.as_posix()


def _now_stamp() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _is_under(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def create_emergency_backup_zip(
    *,
    project_root: Path,
    output_dir_outside_root: Optional[Path] = None,
    exclude_dir_names: Sequence[str] = (".venv", "venv", "__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache"),
) -> Path:
    root = project_root.resolve()
    default_outside = (Path.home() / "HIM_BACKUPS").resolve()
    out_dir = (output_dir_outside_root or default_outside).resolve()
    if _is_under(out_dir, root):
        raise RuntimeError("Backup output directory must be outside the project root.")

    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = (out_dir / f"_emergency_backup_{_now_stamp()}.zip").resolve()

    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for dirpath, dirnames, filenames in os.walk(root):
            d = Path(dirpath)
            dirnames[:] = [x for x in dirnames if x not in set(exclude_dir_names)]
            for fn in filenames:
                p = d / fn
                if not p.is_file():
                    continue
                arcname = _safe_relpath(p, root)
                zf.write(p, arcname=arcname)

    return zip_path


def _collect_junk_dirs(root: Path) -> List[QuarantineItem]:
    junk_names = {
        "analysis_out",
        "old",
        "old_versions",
        "backup",
        "backups",
        "tmp",
        "temp",
        ".cache",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "__pycache__",
    }
    items: List[QuarantineItem] = []
    for name in junk_names:
        p = root / name
        if p.exists() and p.is_dir():
            items.append(QuarantineItem(path=_safe_relpath(p, root), kind="dir", reasons=["junk_dir_name"]))
    return items


def _collect_orphans_via_ast(
    *,
    root: Path,
    profile: str,
    include_tests: bool,
    include_docs: bool,
) -> List[QuarantineItem]:
    try:
        import archive_orphans as ao  # type: ignore
    except Exception as e:
        raise RuntimeError(f"Cannot import archive_orphans.py: {type(e).__name__}: {e}") from e

    report = ao.analyze(root=root, include_tests=include_tests, include_docs=include_docs, profile=profile)
    out: List[QuarantineItem] = []
    for rec in report.get("orphaned", []):
        if not isinstance(rec, dict):
            continue
        p = rec.get("path")
        if not isinstance(p, str) or not p.strip():
            continue
        out.append(
            QuarantineItem(
                path=p,
                kind=str(rec.get("kind") or "unknown"),
                reasons=[str(x) for x in (rec.get("flag_reasons") or []) if isinstance(x, str)],
            )
        )
    return out


def _dedupe_items(items: Sequence[QuarantineItem]) -> List[QuarantineItem]:
    seen = set()
    out: List[QuarantineItem] = []
    for it in items:
        key = (it.path, it.kind)
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


def _move_path_preserve_layout(*, root: Path, quarantine_root: Path, rel_path: str, dry_run: bool) -> Tuple[bool, str]:
    src = (root / Path(rel_path)).resolve()
    if not src.exists():
        return False, "missing"

    dst = (quarantine_root / Path(rel_path)).resolve()
    if not _is_under(dst, quarantine_root):
        return False, "invalid_destination"

    if dry_run:
        return True, "dry_run"

    _ensure_parent_dir(dst)
    shutil.move(str(src), str(dst))
    return True, "moved"


def run_quarantine(
    *,
    root: Path,
    quarantine_dir_name: str,
    profile: str,
    include_tests: bool,
    include_docs: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    quarantine_root = (root / quarantine_dir_name).resolve()

    items = _dedupe_items(_collect_junk_dirs(root) + _collect_orphans_via_ast(root=root, profile=profile, include_tests=include_tests, include_docs=include_docs))

    actions: List[Dict[str, Any]] = []
    for it in items:
        rel = it.path.replace("\\", "/")
        if rel.startswith(quarantine_dir_name + "/") or rel == quarantine_dir_name:
            continue
        ok, status = _move_path_preserve_layout(root=root, quarantine_root=quarantine_root, rel_path=rel, dry_run=dry_run)
        actions.append(
            {
                "path": rel,
                "kind": it.kind,
                "reasons": it.reasons,
                "ok": bool(ok),
                "status": status,
                "src": rel,
                "dst": f"{quarantine_dir_name}/{rel}",
            }
        )

    return {
        "root": root.as_posix(),
        "profile": profile,
        "dry_run": bool(dry_run),
        "quarantine_dir": quarantine_dir_name,
        "items_count": int(len(items)),
        "actions": actions,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--quarantine-dir", default="_QUARANTINE_BIN_")
    ap.add_argument("--profile", choices=["production", "all"], default="production")
    ap.add_argument("--include-tests", action="store_true")
    ap.add_argument("--include-docs", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", default="quarantine_report.json")
    ap.add_argument("--backup-dir", default="")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    backup_dir = Path(args.backup_dir).resolve() if str(args.backup_dir).strip() else None

    backup_zip = create_emergency_backup_zip(project_root=root, output_dir_outside_root=backup_dir)

    rep = run_quarantine(
        root=root,
        quarantine_dir_name=str(args.quarantine_dir),
        profile=str(args.profile),
        include_tests=bool(args.include_tests),
        include_docs=bool(args.include_docs),
        dry_run=bool(args.dry_run),
    )
    rep["emergency_backup_zip"] = backup_zip.as_posix()

    out_path = (root / str(args.report)).resolve()
    _ensure_parent_dir(out_path)
    out_path.write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out_path.as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
