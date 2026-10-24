"""
Run every query in queries.sql against movies.db and print the results as tables.

Usage (from the project root):
    python scripts/run_queries.py               # up to 15 rows per query
    python scripts/run_queries.py --all         # every row (Q1 and Q4 are long)
    python scripts/run_queries.py --rows 5      # up to 5 rows per query

The database is opened read-only, so running this can never change the data.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUERIES_PATH = ROOT / "queries.sql"
DB_PATH = ROOT / "movies.db"
MARKER = "-- @query"


def load_queries(path: Path = QUERIES_PATH) -> list[tuple[str, str]]:
    """Split queries.sql into (title, sql) pairs at each '-- @query' line."""
    queries: list[tuple[str, str]] = []
    title, lines = None, []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith(MARKER):
            if title:
                queries.append((title, "\n".join(lines).strip()))
            title, lines = line[len(MARKER):].strip(), []
        elif title:
            lines.append(line)
    if title:
        queries.append((title, "\n".join(lines).strip()))
    return queries


def format_table(columns: list[str], rows: list[tuple]) -> str:
    """Plain-text table with aligned columns (no extra libraries needed)."""
    text_rows = [["NULL" if v is None else str(v) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in text_rows]) for i, c in enumerate(columns)]
    line = "  ".join(c.ljust(w) for c, w in zip(columns, widths))
    rule = "  ".join("-" * w for w in widths)
    body = ["  ".join(v.ljust(w) for v, w in zip(r, widths)) for r in text_rows]
    return "\n".join([line, rule, *body])


def main() -> None:
    parser = argparse.ArgumentParser(description="Run queries.sql against movies.db")
    parser.add_argument("--rows", type=int, default=15, help="rows to show per query")
    parser.add_argument("--all", action="store_true", help="show every row")
    args = parser.parse_args()

    if not DB_PATH.is_file():
        sys.exit(f"{DB_PATH} not found. Build it first with: python etl.py")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")   # accented titles on any console

    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    for title, sql in load_queries():
        cursor = conn.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        shown = rows if args.all else rows[:args.rows]
        print("=" * 100)
        print(title)
        print("=" * 100)
        print(format_table(columns, shown))
        hidden = len(rows) - len(shown)
        print(f"({len(rows)} row(s)" + (f"; {hidden} more not shown - use --all)" if hidden else ")"))
        print()
    conn.close()


if __name__ == "__main__":
    main()