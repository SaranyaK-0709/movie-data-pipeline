"""
Inspect the OMDb cache in movies.db (read-only).

Usage (from the project root):
    python scripts/inspect_cache.py            # summary + 10 sample rows
    python scripts/inspect_cache.py 25         # summary + 25 sample rows
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "movies.db"


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    if not DB_PATH.is_file():
        sys.exit(f"{DB_PATH} does not exist yet. Run: python etl.py")

    # mode=ro opens the database read-only, so this script can never change it.
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)

    print("Status counts:")
    for status, n in conn.execute(
            "SELECT status, COUNT(*) FROM omdb_cache GROUP BY status ORDER BY status"):
        print(f"    {status:<10} {n:>6}")

    print("\nLookup methods:")
    for method, n in conn.execute(
            "SELECT lookup_method, COUNT(*) FROM omdb_cache GROUP BY lookup_method"):
        print(f"    {method:<10} {n:>6}")

    n_enriched, n_ratings = conn.execute(
        "SELECT COALESCE(SUM(m.enrichment_status = 'enriched'), 0), COUNT(*) "
        "FROM ratings AS r JOIN movies AS m ON m.movie_id = r.movie_id").fetchone()
    if n_ratings:
        print(f"\nRatings on enriched movies: {n_enriched} of {n_ratings} "
              f"({100 * n_enriched / n_ratings:.1f}%)")

    print(f"\nLatest {limit} cached movies:")
    rows = conn.execute(
        """
        SELECT movie_id, imdb_id, status, lookup_method, attempts,
               json_extract(response_json, '$.Title'),
               json_extract(response_json, '$.Year'),
               json_extract(response_json, '$.Director'),
               json_extract(response_json, '$.BoxOffice'),
               error_message
        FROM omdb_cache
        ORDER BY fetched_at DESC, movie_id
        LIMIT ?
        """, (limit,))
    for movie_id, imdb_id, status, method, attempts, title, year, director, box, err in rows:
        detail = f"{title} ({year}) | dir: {director} | box office: {box}" if status == "found" \
            else f"reason: {err}"
        print(f"    {movie_id:>6} {imdb_id or '-':<11} {status:<9} {method:<10} "
              f"x{attempts} | {detail}")
    conn.close()


if __name__ == "__main__":
    main()