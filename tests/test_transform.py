"""
Tests for the Transform stage: value parsers and the combined movie/genre/director/rating rows.

Values are taken from the real data seen during profiling and the first OMDb runs
(e.g. '$292,587,330', '16 Jul 2010', 'Lana Wachowski, Lilly Wachowski').

Run from the project root:
    python -m pytest tests/test_transform.py -v
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

import etl


# ------------------------------------------------------------- value parsers

@pytest.mark.parametrize("value, expected", [
    ("  Hello  ", "Hello"), ("N/A", None), ("", None), (None, None), (float("nan"), None),
])
def test_clean_text(value, expected):
    assert etl.clean_text(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("$292,587,330", 292587330),
    ("$28,767,189", 28767189),
    ("$0", 0),
    ("N/A", None),
    (None, None),
    ("about $5M", None),          # unexpected format -> None, never a wrong number
])
def test_parse_box_office(value, expected):
    assert etl.parse_box_office(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("16 Jul 2010", "2010-07-16"),
    ("01 Jan 1995", "1995-01-01"),
    ("5 Mar 1999", "1999-03-05"),
    ("31 Feb 2000", None),         # impossible date
    ("2010-07-16", None),          # not OMDb's format
    ("N/A", None),
    (None, None),
])
def test_parse_released_date(value, expected):
    assert etl.parse_released_date(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("148 min", 148), ("90min", 90), ("0 min", None), ("N/A", None), ("2 h", None), (None, None),
])
def test_parse_runtime(value, expected):
    assert etl.parse_runtime(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("Christopher Nolan", ["Christopher Nolan"]),
    ("Lana Wachowski, Lilly Wachowski", ["Lana Wachowski", "Lilly Wachowski"]),
    ("Joel Coen, Ethan Coen(uncredited)", ["Joel Coen", "Ethan Coen"]),
    ("Jane Doe, Jane Doe", ["Jane Doe"]),
    ("N/A", []),
    (None, []),
])
def test_split_directors(value, expected):
    assert etl.split_directors(value) == expected


@pytest.mark.parametrize("value, expected", [
    ("Adventure|Animation|Children", ["Adventure", "Animation", "Children"]),
    ("(no genres listed)", []),
    ("Drama|Drama", ["Drama"]),
    (None, []),
])
def test_split_genres(value, expected):
    assert etl.split_genres(value) == expected


@pytest.mark.parametrize("year, decade", [(1995, 1990), (2000, 2000), (1909, 1900), (None, None)])
def test_decade_of(year, decade):
    assert etl.decade_of(year) == decade


def test_unix_timestamps_become_utc_text():
    result = etl.unix_to_utc_text(pd.Series([964982703, 0]))
    assert result.tolist() == ["2000-07-30 18:45:03", "1970-01-01 00:00:00"]


# ------------------------------------------------------------- combined transform

def omdb_body(**fields) -> str:
    body = {"Response": "True", "Year": "1995", "Director": "John Lasseter",
            "Plot": "A cowboy doll...", "BoxOffice": "$229,947,062",
            "Released": "22 Nov 1995", "Runtime": "81 min"}
    body.update(fields)
    return json.dumps(body)


def make_data() -> etl.ExtractedData:
    movies = pd.DataFrame({
        "movieId": [1, 2, 3, 4],
        "title": ["Toy Story (1995)", "Black Mirror", "Matrix, The (1999)", "Unrated (2001)"],
        "genres": ["Adventure|Animation", "Sci-Fi|Drama", "Action|Sci-Fi", "(no genres listed)"],
    })
    links = pd.DataFrame({"movieId": [1, 2, 3, 4],
                          "imdb_id": ["tt0114709", "tt2085059", "tt0133093", None]})
    ratings = pd.DataFrame({"userId": [1, 1, 2], "movieId": [1, 3, 3],
                            "rating": [4.0, 5.0, 3.5],
                            "timestamp": [964982703, 964981247, 1537799250]})
    return etl.ExtractedData(movies=movies, ratings=ratings, links=links)


def make_cache() -> pd.DataFrame:
    return pd.DataFrame([
        (1, "found", omdb_body(), "2026-10-24 09:00:00"),
        (2, "found", omdb_body(Year="2011\u2013", Director="N/A", BoxOffice="N/A",
                               Released="N/A", Runtime="N/A", Plot="N/A"), "2026-10-24 09:00:01"),
        (3, "not_found", json.dumps({"Response": "False", "Error": "Movie not found!"}),
         "2026-10-24 09:00:02"),
    ], columns=["movie_id", "status", "response_json", "fetched_at"])


@pytest.fixture
def result() -> etl.TransformedData:
    return etl.transform(make_data(), make_cache())


def movie(result, movie_id) -> dict:
    return result.movies.set_index("movie_id").loc[movie_id].to_dict()


def test_every_movie_is_kept_even_without_enrichment(result):
    assert sorted(result.movies["movie_id"]) == [1, 2, 3, 4]


def test_enriched_movie_gets_clean_omdb_fields(result):
    m = movie(result, 1)
    assert m["enrichment_status"] == "enriched" and m["enriched_at"] == "2026-10-24 09:00:00"
    assert m["box_office_usd"] == 229947062
    assert m["released_date"] == "1995-11-22"
    assert m["runtime_minutes"] == 81
    assert m["plot"] == "A cowboy doll..."


def test_release_year_prefers_movielens(result):
    m = movie(result, 1)
    assert (m["release_year"], m["release_year_source"], m["release_decade"]) == \
        (1995, "movielens", 1990)


def test_release_year_falls_back_to_omdb_when_title_has_none(result):
    m = movie(result, 2)  # 'Black Mirror' has no year in its MovieLens title
    assert (m["release_year"], m["release_year_source"], m["release_decade"]) == \
        (2011, "omdb", 2010)


def test_na_values_become_missing(result):
    m = movie(result, 2)
    assert pd.isna(m["box_office_usd"]) and pd.isna(m["runtime_minutes"])
    assert pd.isna(m["plot"]) and pd.isna(m["released_date"])


def test_not_found_and_pending_statuses(result):
    assert movie(result, 3)["enrichment_status"] == "not_found"
    assert movie(result, 4)["enrichment_status"] == "pending"
    assert pd.isna(movie(result, 3)["enriched_at"])


def test_not_found_movie_still_has_movielens_year(result):
    assert movie(result, 3)["release_year"] == 1999


def test_missing_imdb_id_stays_missing(result):
    assert pd.isna(movie(result, 4)["imdb_id"])


def test_integer_columns_stay_integers(result):
    for column in ("release_year", "release_decade", "box_office_usd", "runtime_minutes"):
        assert str(result.movies[column].dtype) == "Int64"


def test_genres_become_one_row_per_pair(result):
    pairs = set(map(tuple, result.movie_genres.itertuples(index=False)))
    assert pairs == {(1, "Adventure"), (1, "Animation"), (2, "Sci-Fi"), (2, "Drama"),
                     (3, "Action"), (3, "Sci-Fi")}  # movie 4 '(no genres listed)' -> none


def test_directors_only_from_enriched_movies(result):
    pairs = list(map(tuple, result.movie_directors.itertuples(index=False)))
    assert pairs == [(1, "John Lasseter")]  # movie 2 'N/A', movie 3 not found


def test_co_directors_are_split():
    cache = make_cache()
    cache.loc[0, "response_json"] = omdb_body(Director="Lana Wachowski, Lilly Wachowski")
    result = etl.transform(make_data(), cache)
    assert result.movie_directors["director_name"].tolist() == ["Lana Wachowski", "Lilly Wachowski"]


def test_corrupt_cached_json_is_ignored_not_fatal():
    cache = make_cache()
    cache.loc[0, "response_json"] = "{not json"
    m = movie(etl.transform(make_data(), cache), 1)
    assert pd.isna(m["box_office_usd"])
    assert m["release_year"] == 1995  # MovieLens data is still used


def test_ratings_are_typed_and_timestamps_converted(result):
    r = result.ratings
    assert list(r.columns) == ["user_id", "movie_id", "rating", "rated_at"]
    assert r["rated_at"].tolist()[0] == "2000-07-30 18:45:03"
    assert len(r) == 3


def test_transformed_movies_satisfy_the_schema(tmp_path, result):
    """Every transformed row must be accepted by the real schema's CHECK constraints."""
    conn = etl.connect_db(tmp_path / "check.db")
    rows = result.movies.astype(object).where(result.movies.notna(), None)
    conn.executemany(
        f"INSERT INTO movies ({', '.join(rows.columns)}) VALUES ({', '.join('?' * len(rows.columns))})",
        rows.itertuples(index=False, name=None),
    )
    assert conn.execute("SELECT COUNT(*) FROM movies").fetchone()[0] == 4
    conn.close()