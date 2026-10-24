"""
Tests for the Enrich stage: OMDb client, movie matching, cache and resumability.

No test touches the real OMDb API. A FakeSession stands in for `requests`,
returning prepared responses and recording every request made, and sleeping
is replaced by a no-op so retries run instantly.

Run from the project root:
    python -m pytest tests/test_enrich.py -v
"""

from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest
import requests

import etl

KEY = "testkey1"


# ------------------------------------------------------------- test doubles

class FakeResponse:
    def __init__(self, status_code: int = 200, body=None, text: str | None = None):
        self.status_code = status_code
        self._body = body
        self._text = text

    def json(self):
        if self._text is not None:          # simulate a non-JSON body
            raise ValueError("not JSON")
        return self._body


class FakeSession:
    """Replays `responses` in order; an Exception instance in the list is raised."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests: list[dict] = []

    def get(self, url, params, timeout):
        self.requests.append(dict(params))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def found(imdb_id="tt0114709", title="Toy Story", year="1995"):
    return FakeResponse(200, {"Response": "True", "imdbID": imdb_id, "Title": title,
                              "Year": year, "Director": "John Lasseter"})


NOT_FOUND = FakeResponse(200, {"Response": "False", "Error": "Movie not found!"})
BAD_ID = FakeResponse(200, {"Response": "False", "Error": "Incorrect IMDb ID."})
INVALID_KEY = FakeResponse(401, {"Response": "False", "Error": "Invalid API key!"})
LIMIT = FakeResponse(401, {"Response": "False", "Error": "Request limit reached!"})


def client(responses, max_calls=100):
    session = FakeSession(responses)
    return etl.OmdbClient(KEY, max_calls, session=session, sleep=lambda s: None), session


# ------------------------------------------------------------- OmdbClient

def test_found_response():
    c, session = client([found()])
    result = c.fetch({"i": "tt0114709"})
    assert result.status == "found" and result.data["Director"] == "John Lasseter"
    assert session.requests[0]["apikey"] == KEY and c.calls_made == 1


def test_not_found_is_http_200_with_response_false():
    c, _ = client([NOT_FOUND])
    result = c.fetch({"i": "tt0000001"})
    assert result.status == "not_found" and result.error == "Movie not found!"


def test_timeout_is_retried_then_succeeds():
    c, _ = client([requests.Timeout(), found()])
    assert c.fetch({"i": "tt0114709"}).status == "found"
    assert c.calls_made == 2              # the failed try still used quota


def test_server_error_is_retried():
    c, _ = client([FakeResponse(503, None, text="Service Unavailable"), found()])
    assert c.fetch({"i": "tt0114709"}).status == "found"


def test_repeated_timeouts_give_error_after_max_attempts():
    c, _ = client([requests.Timeout()] * etl.OMDB_MAX_ATTEMPTS)
    result = c.fetch({"i": "tt0114709"})
    assert result.status == "error" and "timeout" in result.error
    assert c.calls_made == etl.OMDB_MAX_ATTEMPTS


def test_malformed_json_is_retried_then_reported():
    bad = FakeResponse(200, None, text="<html>oops</html>")
    c, _ = client([bad] * etl.OMDB_MAX_ATTEMPTS)
    assert c.fetch({"i": "tt0114709"}).status == "error"


def test_json_without_response_field_is_an_error():
    c, _ = client([FakeResponse(200, {"Title": "Odd"})])
    result = c.fetch({"i": "tt0114709"})
    assert result.status == "error" and "Response" in result.error


def test_invalid_key_raises_immediately():
    c, _ = client([INVALID_KEY])
    with pytest.raises(etl.OmdbAuthError):
        c.fetch({"i": "tt0114709"})


@pytest.mark.parametrize("response", [LIMIT, FakeResponse(429, None, text="Too Many")])
def test_rate_limit_raises(response):
    c, _ = client([response])
    with pytest.raises(etl.OmdbRateLimitError):
        c.fetch({"i": "tt0114709"})


def test_budget_is_never_exceeded():
    c, session = client([found()], max_calls=1)
    c.fetch({"i": "tt0114709"})
    with pytest.raises(etl.ApiBudgetExhausted):
        c.fetch({"i": "tt0113497"})
    assert len(session.requests) == 1


def test_api_key_is_redacted_from_connection_errors():
    url = f"https://www.omdbapi.com/?i=tt0114709&apikey={KEY}"
    c, _ = client([requests.ConnectionError(f"Max retries exceeded with url: {url}")] * 3)
    result = c.fetch({"i": "tt0114709"})
    assert KEY not in result.error and "***" in result.error


# ------------------------------------------------------------- title helpers

@pytest.mark.parametrize("title, expected", [
    ("Toy Story (1995)", "Toy Story"),
    ("American President, The (1995)", "The American President"),
    ("Seven (a.k.a. Se7en) (1995)", "Seven"),
    ("Postman, The (Postino, Il) (1994)", "The Postman"),
    ("Mis\u00e9rables, Les (1995)", "Les Mis\u00e9rables"),
    ("Good, the Bad and the Ugly, The (Buono, il brutto, il cattivo, Il) (1966)",
     "The Good, the Bad and the Ugly"),
    ("Black Mirror", "Black Mirror"),
    ("  Padded (2000)  ", "Padded"),
])
def test_search_title(title, expected):
    assert etl.search_title(title) == expected


@pytest.mark.parametrize("title, year", [
    ("Toy Story (1995)", 1995),
    ("Death Note: Desu n\u00f4to (2006\u20132007)", 2006),
    ("Black Mirror", None),
    ("Seven (a.k.a. Se7en) (1995)", 1995),
])
def test_year_from_title(title, year):
    assert etl.year_from_title(title) == year


@pytest.mark.parametrize("value, year", [("1995", 1995), ("2006\u20132007", 2006),
                                         ("N/A", None), (None, None)])
def test_parse_omdb_year(value, year):
    assert etl.parse_omdb_year(value) == year


# ------------------------------------------------------------- matching strategy

def test_lookup_by_imdb_id_uses_one_call():
    c, session = client([found()])
    e = etl.lookup_movie(c, 1, "Toy Story (1995)", "tt0114709")
    assert e.status == "found" and e.lookup_method == "imdb_id"
    assert session.requests == [{"i": "tt0114709", "apikey": KEY}]


def test_falls_back_to_title_and_year_when_id_not_found():
    c, session = client([BAD_ID, found(imdb_id="tt0112346", title="The American President")])
    e = etl.lookup_movie(c, 11, "American President, The (1995)", "tt9999999")
    assert e.status == "found" and e.lookup_method == "title_year"
    assert session.requests[1]["t"] == "The American President"
    assert session.requests[1]["y"] == "1995"


def test_movie_without_imdb_id_uses_title_search():
    c, session = client([found()])
    e = etl.lookup_movie(c, 1, "Toy Story (1995)", None)
    assert e.status == "found" and e.lookup_method == "title_year"
    assert "i" not in session.requests[0]


def test_title_match_with_wrong_year_is_rejected():
    c, _ = client([NOT_FOUND, found(year="2012")])
    e = etl.lookup_movie(c, 73, "Mis\u00e9rables, Les (1995)", "tt0113828")
    assert e.status == "not_found" and "rejected" in e.error_message


def test_title_match_within_one_year_is_accepted():
    c, _ = client([NOT_FOUND, found(year="1996")])
    assert etl.lookup_movie(c, 1, "Toy Story (1995)", "tt0114709").status == "found"


def test_no_title_search_without_year():
    c, session = client([NOT_FOUND])
    e = etl.lookup_movie(c, 5, "Black Mirror", "tt2085059")
    assert e.status == "not_found" and len(session.requests) == 1
    assert "no title search" in e.error_message


def test_no_request_at_all_without_id_or_year():
    c, session = client([])
    e = etl.lookup_movie(c, 5, "Black Mirror", None)
    assert e.status == "not_found" and e.lookup_method == "none"
    assert session.requests == []


def test_error_on_id_lookup_does_not_fall_back():
    c, session = client([requests.Timeout()] * etl.OMDB_MAX_ATTEMPTS)
    e = etl.lookup_movie(c, 1, "Toy Story (1995)", "tt0114709")
    assert e.status == "error" and all("i" in r for r in session.requests)


# ------------------------------------------------------------- enrich stage (cache + resume)

@pytest.fixture
def conn(tmp_path):
    connection = etl.connect_db(tmp_path / "test.db")
    yield connection
    connection.close()


def make_data(n_movies=4) -> etl.ExtractedData:
    """Movies 1..n with IMDb IDs; movie k has k ratings (so movie n is the most rated)."""
    movies = pd.DataFrame({"movieId": range(1, n_movies + 1),
                           "title": [f"Movie {i} (200{i})" for i in range(1, n_movies + 1)],
                           "genres": ["Drama"] * n_movies})
    links = pd.DataFrame({"movieId": range(1, n_movies + 1),
                          "imdb_id": [f"tt000000{i}" for i in range(1, n_movies + 1)]})
    rows = [(u, m, 4.0, 964982703) for m in range(1, n_movies + 1) for u in range(1, m + 1)]
    ratings = pd.DataFrame(rows, columns=["userId", "movieId", "rating", "timestamp"])
    return etl.ExtractedData(movies=movies, ratings=ratings, links=links)


def make_config(tmp_path, max_calls=100, key=KEY, skip=False) -> etl.Config:
    return etl.Config(data_dir=tmp_path, db_path=tmp_path / "test.db", log_dir=tmp_path,
                      max_api_calls=max_calls, skip_api=skip, omdb_api_key=key)


def cache_rows(conn):
    return conn.execute("SELECT movie_id, status, attempts FROM omdb_cache "
                        "ORDER BY movie_id").fetchall()


def run_enrich(conn, data, config, responses):
    session = FakeSession(responses)
    stats = etl.enrich_with_omdb(conn, data, config, session=session, sleep=lambda s: None)
    return stats, session


def test_most_rated_movies_are_looked_up_first(conn, tmp_path):
    stats, session = run_enrich(conn, make_data(), make_config(tmp_path, max_calls=2),
                                [found(), found()])
    assert [r["i"] for r in session.requests] == ["tt0000004", "tt0000003"]
    assert stats.found == 2 and stats.remaining == 2 and "budget" in stats.stopped_reason


def test_results_are_cached_with_raw_json(conn, tmp_path):
    run_enrich(conn, make_data(1), make_config(tmp_path), [found(imdb_id="tt0000001")])
    status, body = conn.execute("SELECT status, response_json FROM omdb_cache").fetchone()
    assert status == "found" and json.loads(body)["Director"] == "John Lasseter"


def test_second_run_resumes_without_repeating_calls(conn, tmp_path):
    data, config = make_data(), make_config(tmp_path, max_calls=2)
    run_enrich(conn, data, config, [found(), found()])
    stats, session = run_enrich(conn, data, config, [found(), found()])
    assert [r["i"] for r in session.requests] == ["tt0000002", "tt0000001"]
    assert stats.remaining == 0 and len(cache_rows(conn)) == 4


def test_fully_cached_run_makes_no_calls(conn, tmp_path):
    data, config = make_data(2), make_config(tmp_path)
    run_enrich(conn, data, config, [found(), found()])
    stats, session = run_enrich(conn, data, config, [])
    assert session.requests == [] and "nothing to do" in stats.stopped_reason


def test_not_found_is_final_but_errors_are_retried(conn, tmp_path):
    data, config = make_data(2), make_config(tmp_path)
    timeouts = [requests.Timeout()] * etl.OMDB_MAX_ATTEMPTS
    # movie 2 (most rated): not found and no fallback hit; movie 1: error
    run_enrich(conn, data, config, [NOT_FOUND, NOT_FOUND] + timeouts)
    assert cache_rows(conn) == [(1, "error", 1), (2, "not_found", 1)]

    stats, session = run_enrich(conn, data, config, [found()])
    assert [r["i"] for r in session.requests] == ["tt0000001"]   # only the error is retried
    assert cache_rows(conn) == [(1, "found", 2), (2, "not_found", 1)]


def test_persistent_errors_stop_being_retried(conn, tmp_path):
    data, config = make_data(1), make_config(tmp_path)
    for _ in range(etl.MAX_ERROR_RUNS):
        run_enrich(conn, data, config, [requests.Timeout()] * etl.OMDB_MAX_ATTEMPTS)
    stats, session = run_enrich(conn, data, config, [])
    assert session.requests == [] and cache_rows(conn) == [(1, "error", etl.MAX_ERROR_RUNS)]


def test_rate_limit_stops_run_but_keeps_progress(conn, tmp_path):
    stats, _ = run_enrich(conn, make_data(), make_config(tmp_path), [found(), LIMIT])
    assert "limit" in stats.stopped_reason
    assert cache_rows(conn) == [(4, "found", 1)]   # the first result was saved


def test_invalid_key_stops_run(conn, tmp_path):
    stats, session = run_enrich(conn, make_data(), make_config(tmp_path), [INVALID_KEY])
    assert len(session.requests) == 1 and "API key" in stats.stopped_reason
    assert cache_rows(conn) == []


@pytest.mark.parametrize("kwargs, reason", [
    ({"skip": True}, "--skip-api"),
    ({"key": None}, "no OMDB_API_KEY"),
    ({"max_calls": 0}, "max_api_calls = 0"),
])
def test_enrichment_can_be_skipped(conn, tmp_path, kwargs, reason):
    stats, session = run_enrich(conn, make_data(), make_config(tmp_path, **kwargs), [])
    assert session.requests == [] and reason in stats.stopped_reason


def test_connect_db_enforces_foreign_keys(tmp_path):
    connection = etl.connect_db(tmp_path / "fk.db")
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("INSERT INTO ratings VALUES (1, 999, 4.0, '2000-01-01 00:00:00')")
    connection.close()


def test_real_network_is_blocked_during_tests():
    """conftest.py must stop any test from reaching the real OMDb API."""
    with pytest.raises(RuntimeError, match="disabled"):
        requests.get(etl.OMDB_URL, params={"i": "tt0114709"}, timeout=1)


def test_retries_are_counted_and_reported(conn, tmp_path):
    """A temporary failure followed by success is visible in the stats (and the log)."""
    stats, _ = run_enrich(conn, make_data(1), make_config(tmp_path), [requests.Timeout(), found()])
    assert stats.found == 1 and stats.api_calls == 2 and stats.retries == 1
