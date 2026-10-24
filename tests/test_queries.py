"""
Tests for queries.sql: every query is run against a small database whose
correct answers were calculated by hand.

Run from the project root:
    python -m pytest tests/test_queries.py -v
"""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

import etl

ROOT = Path(__file__).resolve().parent.parent

# Load scripts/run_queries.py so the tests split queries.sql exactly as the runner does.
_spec = importlib.util.spec_from_file_location("run_queries", ROOT / "scripts" / "run_queries.py")
run_queries = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(run_queries)
QUERIES = {title.split(" |")[0]: sql for title, sql in run_queries.load_queries()}

# Hand-built data (movie_id, title, release_year, enrichment_status):
#   1 Alpha  (1995): ratings 5.0              -> avg 5.0  (1 rating)
#   2 Beta   (1995): ratings 5.0              -> avg 5.0  (1 rating)   tie with Alpha
#   3 Gamma  (1999): ratings 4.0, 5.0, 4.5    -> avg 4.5  (3 ratings)
#   4 Delta  (1999): ratings 1.0, 2.0         -> avg 1.5  (2 ratings)
#   5 Eps    (year unknown): rating 3.0
#   6 Zeta   (2001): no ratings
MOVIES = [(1, "Alpha (1995)", 1995), (2, "Beta (1995)", 1995), (3, "Gamma (1999)", 1999),
          (4, "Delta (1999)", 1999), (5, "Eps", None), (6, "Zeta (2001)", 2001)]
RATINGS = [(1, 1, 5.0), (2, 2, 5.0), (1, 3, 4.0), (2, 3, 5.0), (3, 3, 4.5),
           (1, 4, 1.0), (2, 4, 2.0), (1, 5, 3.0)]
GENRES = {1: ["Drama"], 2: ["Comedy"], 3: ["Drama", "Comedy"], 4: ["Horror"], 5: ["Horror"]}
DIRECTORS = {1: ["Ann Lee"], 2: ["Bo Kim"], 3: ["Ann Lee", "Bo Kim"], 4: ["Cy Dot"]}


@pytest.fixture
def conn(tmp_path):
    c = etl.connect_db(tmp_path / "q.db")
    with c:
        for movie_id, title, year in MOVIES:
            c.execute("INSERT INTO movies (movie_id, imdb_id, title, release_year, "
                      "release_year_source, release_decade, enrichment_status) "
                      "VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (movie_id, f"tt000000{movie_id}", title, year,
                       "movielens" if year else None, etl.decade_of(year),
                       "enriched" if movie_id in DIRECTORS else "pending"))
        c.executemany("INSERT INTO ratings VALUES (?, ?, ?, '2000-01-01 00:00:00')", RATINGS)
        for table, id_col, name_col, link, mapping in (
                ("genres", "genre_id", "genre_name", "movie_genres", GENRES),
                ("directors", "director_id", "director_name", "movie_directors", DIRECTORS)):
            for movie_id, names in mapping.items():
                for name in names:
                    c.execute(f"INSERT INTO {table} ({name_col}) VALUES (?) "
                              f"ON CONFLICT ({name_col}) DO NOTHING", (name,))
                    c.execute(f"INSERT INTO {link} SELECT ?, {id_col} FROM {table} "
                              f"WHERE {name_col} = ?", (movie_id, name))
    yield c
    c.close()


def run(conn, key, sql=None):
    cursor = conn.execute(sql or QUERIES[key])
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def test_queries_file_has_all_questions():
    assert list(QUERIES) == ["Q1", "Q1b", "Q2", "Q3", "Q4"]


def test_q1_returns_every_tied_movie(conn):
    rows = run(conn, "Q1")
    assert [(r["title"], r["avg_rating"], r["n_ratings"]) for r in rows] == \
        [("Alpha (1995)", 5.0, 1), ("Beta (1995)", 5.0, 1)]


def test_q1b_applies_the_minimum_ratings_threshold(conn):
    sql = QUERIES["Q1b"].replace("50 AS min_ratings", "2 AS min_ratings")
    rows = run(conn, "Q1b", sql)
    assert [(r["position"], r["title"], r["avg_rating"]) for r in rows] == \
        [(1, "Gamma (1999)", 4.5), (2, "Delta (1999)", 1.5)]


def test_q1b_default_threshold_is_50(conn):
    assert "50 AS min_ratings" in QUERIES["Q1b"]
    assert run(conn, "Q1b") == []        # no movie in the sample has 50 ratings


def test_q2_ranks_genres_by_average_of_all_their_ratings(conn):
    rows = run(conn, "Q2")
    # Drama: 5.0, 4.0, 5.0, 4.5 -> 4.625 | Comedy: 5.0, 4.0, 5.0, 4.5 -> 4.625 (tie)
    # Horror: 1.0, 2.0, 3.0 -> 2.0
    assert [(r["position"], r["genre_name"], r["avg_rating"], r["n_ratings"], r["n_movies"])
            for r in rows] == [(1, "Comedy", 4.625, 4, 2), (1, "Drama", 4.625, 4, 2),
                               (3, "Horror", 2.0, 3, 2)]


def test_q2_also_reports_average_of_movie_averages(conn):
    horror = [r for r in run(conn, "Q2") if r["genre_name"] == "Horror"][0]
    assert horror["avg_of_movie_averages"] == 2.25    # (1.5 + 3.0) / 2


def test_q3_counts_co_directed_films_for_each_director(conn):
    rows = run(conn, "Q3")
    # Ann Lee: movies 1, 3 | Bo Kim: movies 2, 3 | Cy Dot: movie 4
    assert [(r["director_name"], r["n_movies"]) for r in rows] == [("Ann Lee", 2), ("Bo Kim", 2)]
    assert (rows[0]["enriched_movies"], rows[0]["total_movies"]) == (4, 6)


def test_q4_groups_by_release_year_and_skips_unknown_years(conn):
    rows = run(conn, "Q4")
    assert [(r["release_year"], r["avg_rating"], r["n_ratings"], r["n_movies"],
             r["avg_of_movie_averages"]) for r in rows] == [
        (1995, 5.0, 2, 2, 5.0),
        (1999, 3.3, 5, 2, 3.0),      # (4.0+5.0+4.5+1.0+2.0)/5 = 3.3 ; (4.5+1.5)/2 = 3.0
    ]                                # 2001 has no ratings; 'Eps' has no year


def test_runner_formats_a_table():
    text = run_queries.format_table(["a", "bb"], [(1, None), (22, "x")])
    assert text.splitlines() == ["a   bb  ", "--  ----", "1   NULL", "22  x   "]