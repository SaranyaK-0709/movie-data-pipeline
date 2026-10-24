"""
Tests for the Load stage: idempotency, upserts, link synchronisation, transactions,
and an end-to-end run of the whole pipeline (twice) on small sample files.

Run from the project root:
    python -m pytest tests/test_load.py -v
"""

from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest

import etl

MOVIES = (
    "movieId,title,genres\n"
    "1,Toy Story (1995),Adventure|Animation\n"
    '2571,"Matrix, The (1999)",Action|Sci-Fi\n'
    "5,Black Mirror,(no genres listed)\n"
)
RATINGS = (
    "userId,movieId,rating,timestamp\n"
    "1,1,4.0,964982703\n"
    "1,2571,5.0,964981247\n"
    "2,2571,3.5,1537799250\n"
)
LINKS = "movieId,imdbId,tmdbId\n1,0114709,862\n2571,0133093,603\n5,2085059,\n"


def cache_frame() -> pd.DataFrame:
    """Cached OMDb results: Toy Story and The Matrix found, Black Mirror not looked up."""
    toy = {"Response": "True", "Year": "1995", "Director": "John Lasseter",
           "BoxOffice": "$229,947,062", "Released": "22 Nov 1995", "Runtime": "81 min",
           "Plot": "A cowboy doll..."}
    matrix = {"Response": "True", "Year": "1999", "Director": "Lana Wachowski, Lilly Wachowski",
              "BoxOffice": "N/A", "Released": "31 Mar 1999", "Runtime": "136 min", "Plot": "N/A"}
    return pd.DataFrame([(1, "found", json.dumps(toy), "2026-10-24 09:00:00"),
                         (2571, "found", json.dumps(matrix), "2026-10-24 09:00:01")],
                        columns=["movie_id", "status", "response_json", "fetched_at"])


@pytest.fixture
def data_dir(tmp_path):
    folder = tmp_path / "data"
    folder.mkdir()
    for name, text in (("movies.csv", MOVIES), ("ratings.csv", RATINGS), ("links.csv", LINKS)):
        (folder / name).write_text(text, encoding="utf-8")
    return folder


@pytest.fixture
def conn(tmp_path):
    connection = etl.connect_db(tmp_path / "test.db")
    yield connection
    connection.close()


def transformed(data_dir, cache=None) -> etl.TransformedData:
    return etl.transform(etl.extract_csv_sources(data_dir),
                         cache if cache is not None else cache_frame())


def dump(conn) -> dict[str, list]:
    """Full, ordered contents of every table: used to prove two runs give identical data."""
    return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2").fetchall() for t in etl.TABLES}


# ------------------------------------------------------------- first load

def test_first_load_writes_every_table(conn, data_dir):
    stats = etl.load(conn, transformed(data_dir))
    assert stats.counts_after == {"movies": 3, "genres": 4, "movie_genres": 4, "directors": 3,
                                  "movie_directors": 3, "ratings": 3}


def test_missing_values_are_stored_as_sql_null(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    box, plot = conn.execute(
        "SELECT box_office_usd, plot FROM movies WHERE movie_id = 2571").fetchone()
    assert box is None and plot is None


def test_values_are_stored_with_correct_sql_types(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    types = conn.execute("SELECT typeof(release_year), typeof(box_office_usd), typeof(title) "
                         "FROM movies WHERE movie_id = 1").fetchone()
    assert types == ("integer", "integer", "text")
    assert conn.execute("SELECT typeof(rating), rated_at FROM ratings "
                        "WHERE user_id = 1 AND movie_id = 1").fetchone() == \
        ("real", "2000-07-30 18:45:03")


def test_co_directors_are_separate_directors(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    names = [r[0] for r in conn.execute(
        "SELECT d.director_name FROM movie_directors md JOIN directors d USING (director_id) "
        "WHERE md.movie_id = 2571 ORDER BY 1")]
    assert names == ["Lana Wachowski", "Lilly Wachowski"]


def test_unenriched_movie_is_still_loaded(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    assert conn.execute("SELECT enrichment_status, release_year FROM movies "
                        "WHERE movie_id = 5").fetchone() == ("pending", None)


# ------------------------------------------------------------- idempotency

def test_second_load_changes_nothing(conn, data_dir):
    t = transformed(data_dir)
    etl.load(conn, t)
    first = dump(conn)

    stats = etl.load(conn, t)
    assert stats.movies_written == 0 and stats.ratings_written == 0
    assert stats.genre_links == (0, 0) and stats.director_links == (0, 0)
    assert dump(conn) == first          # identical contents, IDs included


def test_genre_ids_are_stable_across_runs(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    ids_before = dict(conn.execute("SELECT genre_name, genre_id FROM genres").fetchall())
    etl.load(conn, transformed(data_dir))
    assert dict(conn.execute("SELECT genre_name, genre_id FROM genres").fetchall()) == ids_before


def test_whole_pipeline_run_twice_gives_identical_database(tmp_path, data_dir, monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "testkey1")
    db = tmp_path / "pipeline.db"
    args = ["--data-dir", str(data_dir), "--db", str(db), "--log-dir", str(tmp_path / "logs"),
            "--skip-api"]
    assert etl.main(args) == 0
    with sqlite3.connect(db) as c:
        first = dump(c)
    assert etl.main(args) == 0
    with sqlite3.connect(db) as c:
        assert dump(c) == first
    log = (tmp_path / "logs" / "pipeline.log").read_text(encoding="utf-8")
    assert "no changes - the database already matched the source" in log
    assert "integrity_check=ok" in log


# ------------------------------------------------------------- updates from the source

def test_changed_rating_is_updated_not_duplicated(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    (data_dir / "ratings.csv").write_text(RATINGS.replace("1,1,4.0,964982703", "1,1,2.5,999999999"),
                                          encoding="utf-8")
    stats = etl.load(conn, transformed(data_dir))
    assert stats.ratings_written == 1 and stats.counts_after["ratings"] == 3
    assert conn.execute("SELECT rating FROM ratings WHERE user_id = 1 AND movie_id = 1"
                        ).fetchone()[0] == 2.5


def test_new_enrichment_updates_existing_movie(conn, data_dir):
    cache = cache_frame()
    etl.load(conn, transformed(data_dir, cache))
    bm = {"Response": "True", "Year": "2011\u2013", "Director": "N/A", "Runtime": "60 min"}
    cache.loc[len(cache)] = [5, "found", json.dumps(bm), "2026-10-25 09:00:00"]
    stats = etl.load(conn, transformed(data_dir, cache))
    assert stats.movies_written == 1
    assert conn.execute("SELECT enrichment_status, release_year, release_year_source "
                        "FROM movies WHERE movie_id = 5").fetchone() == ("enriched", 2011, "omdb")


def test_changed_genres_are_synchronised(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    (data_dir / "movies.csv").write_text(MOVIES.replace("Adventure|Animation", "Animation|Comedy"),
                                         encoding="utf-8")
    stats = etl.load(conn, transformed(data_dir))
    assert stats.genre_links == (1, 1)            # +Comedy, -Adventure
    genres = {r[0] for r in conn.execute("SELECT genre_name FROM genres")}
    assert "Adventure" not in genres and "Comedy" in genres   # unused genre removed


# ------------------------------------------------------------- transactions

def test_failed_load_is_rolled_back_completely(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    before = dump(conn)

    t = transformed(data_dir)
    t.movies.loc[t.movies["movie_id"] == 1, "title"] = "Changed title (1995)"
    t.ratings.loc[len(t.ratings)] = [9, 99999, 4.0, "2000-01-01 00:00:00"]  # unknown movie
    with pytest.raises(sqlite3.IntegrityError):
        etl.load(conn, t)

    assert dump(conn) == before   # the title change before the failure was undone too


def test_integrity_check_passes_after_load(conn, data_dir):
    etl.load(conn, transformed(data_dir))
    etl.check_integrity(conn)     # raises LoadError if anything is wrong