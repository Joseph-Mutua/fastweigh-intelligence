from __future__ import annotations

from pathlib import Path


class QueryLoadError(RuntimeError):
    pass


def load_query(path: str) -> str:
    query_path = Path(path)
    if not query_path.exists():
        raise QueryLoadError(f"Query file not found: {path}")
    content = query_path.read_text(encoding="utf-8").strip()
    if not content:
        raise QueryLoadError(f"Query file is empty: {path}")
    return content
