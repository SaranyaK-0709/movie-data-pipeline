"""
Tests for schema.sql.

Each test builds a fresh in-memory SQLite database from schema.sql, so the
tests never touch the real movies.db and leave nothing behind.

Run from the project root:
    python -m pytest tests/test_schema.py -v
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schema.sql"

EXPECTED_TABLES = {
    "movies", "genres", "movie_genres", "directors",
    "movie_directors", "ratings", "omdb_cache",
}


def apply_schema(conn: sqlite3.Connection) -> None:
    """Run schema.sql against a connection."""
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def conn():
    """A fresh in-memory database with the schema applied and FKs enforced."""
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    apply_schema(connection)
    yield connection
    connection.close()


def insert_movie(conn, movie_id=1, imdb_id="tt0114709", title="Toy Story (1995)",
                 year=1995, source="movielens", decade=1990):
    conn.execute(
        "INSERT INTO movies (movie_id, imdb_id, title, release_year, "
        "release_year_source, release_decade) VALUES (?, ?, ?, ?, ?, ?)",
        (movie_id, imdb_id, title, year, source, decade),
    )


# ---------------------------------------------------------------- structure

def test_sqlite_version_supports_strict_tables():
    """STRICT tables need SQLite 3.37 or newer."""
    assert sqlite3.sqlite_version_info >= (3, 37, 0)


def test_all_tables_created(conn):
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    assert EXPECTED_TABLES <= {name for (name,) in rows}


def test_schema_can_be_applied_twice(conn):
    """IF NOT EXISTS makes re-running the schema safe and keeps existing data."""
    insert_movie(conn)
    apply_schema(conn)  # second application must not fail or wipe data
    assert conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 1


# ---------------------------------------------------------------- movies

def test_valid_movie_insert_uses_defaults(conn):
    insert_movie(conn)
    status = conn.execute("SELECT enrichment_status FROM movies").fetchone()[0]
    assert status == "pending"


def test_movie_without_year_is_allowed(conn):
    """13 real titles have no year (e.g. 'Black Mirror'), so NULL must be allowed."""
    insert_movie(conn, title="Black Mirror", year=None, source=None, decade=None)


@pytest.mark.parametrize("kwargs", [
    {"imdb_id": "114709"},                    # missing 'tt' prefix
    {"imdb_id": "tt123"},                     # too short
    {"title": "   "},                         # blank title
    {"year": 1500, "decade": 1500},           # implausible year
    {"year": 1995, "decade": 2000},           # decade inconsistent with year
    {"year": 1995, "source": None},           # year without a source
    {"year": None, "source": "omdb", "decade": None},  # source without a year
    {"source": "wikipedia"},                  # unknown source
])
def test_invalid_movies_rejected(conn, kwargs):
    with pytest.raises(sqlite3.IntegrityError):
        insert_movie(conn, **kwargs)


def test_duplicate_movie_id_rejected(conn):
    insert_movie(conn)
    with pytest.raises(sqlite3.IntegrityError):
        insert_movie(conn, imdb_id="tt9999999")


def test_duplicate_imdb_id_rejected(conn):
    insert_movie(conn)
    with pytest.raises(sqlite3.IntegrityError):
        insert_movie(conn, movie_id=2)


def test_invalid_enrichment_status_rejected(conn):
    insert_movie(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("UPDATE movies SET enrichment_status = 'done'")


def test_strict_table_rejects_wrong_type(conn):
    """Without STRICT, SQLite would silently store text in an INTEGER column."""
    with pytest.raises(sqlite3.IntegrityError):
        insert_movie(conn, year="nineteen ninety-five")


# ---------------------------------------------------------------- ratings

@pytest.mark.parametrize("rating", [0.5, 3.5, 5.0])
def test_valid_ratings_accepted(conn, rating):
    insert_movie(conn)
    conn.execute("INSERT INTO ratings VALUES (1, 1, ?, '2000-07-30 18:45:03')", (rating,))


@pytest.mark.parametrize("rating", [0.0, 5.5, 3.3, -1.0])
def test_invalid_ratings_rejected(conn, rating):
    insert_movie(conn)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO ratings VALUES (1, 1, ?, '2000-07-30 18:45:03')", (rating,))


def test_duplicate_rating_rejected(conn):
    insert_movie(conn)
    conn.execute("INSERT INTO ratings VALUES (1, 1, 4.0, '2000-07-30 18:45:03')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO ratings VALUES (1, 1, 2.0, '2001-01-01 00:00:00')")


# ---------------------------------------------------------------- foreign keys

def test_rating_for_unknown_movie_rejected(conn):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO ratings VALUES (1, 999, 4.0, '2000-07-30 18:45:03')")


def test_movie_genre_requires_existing_movie_and_genre(conn):
    insert_movie(conn)
    conn.execute("INSERT INTO genres (genre_id, genre_name) VALUES (1, 'Comedy')")
    conn.execute("INSERT INTO movie_genres VALUES (1, 1)")  # valid pair
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO movie_genres VALUES (1, 99)")  # unknown genre
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO movie_genres VALUES (99, 1)")  # unknown movie


def test_duplicate_movie_genre_pair_rejected(conn):
    insert_movie(conn)
    conn.execute("INSERT INTO genres (genre_id, genre_name) VALUES (1, 'Comedy')")
    conn.execute("INSERT INTO movie_genres VALUES (1, 1)")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO movie_genres VALUES (1, 1)")


def test_duplicate_genre_name_rejected(conn):
    conn.execute("INSERT INTO genres (genre_name) VALUES ('Drama')")
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute("INSERT INTO genres (genre_name) VALUES ('Drama')")


def test_co_directors_both_linked(conn):
    insert_movie(conn, movie_id=2571, imdb_id="tt0133093", title="Matrix, The (1999)")
    conn.executemany("INSERT INTO directors (director_id, director_name) VALUES (?, ?)",
                     [(1, "Lana Wachowski"), (2, "Lilly Wachowski")])
    conn.executemany("INSERT INTO movie_directors VALUES (?, ?)", [(2571, 1), (2571, 2)])
    count = conn.execute("SELECT COUNT(*) FROM movie_directors WHERE movie_id = 2571").fetchone()[0]
    assert count == 2


# ---------------------------------------------------------------- omdb_cache

def test_cache_accepts_valid_row(conn):
    conn.execute(
        "INSERT INTO omdb_cache (imdb_id, status, lookup_method, http_status, fetched_at) "
        "VALUES ('tt0114709', 'found', 'imdb_id', 200, '2026-10-24 08:00:00')"
    )


@pytest.mark.parametrize("status, method", [("ok", "imdb_id"), ("found", "guess")])
def test_cache_rejects_invalid_values(conn, status, method):
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO omdb_cache (imdb_id, status, lookup_method, fetched_at) "
            "VALUES ('tt0114709', ?, ?, '2026-10-24 08:00:00')", (status, method),
        )