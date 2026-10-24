"""
Tests for the Extract stage (CSV reading and validation) and configuration in etl.py.

Every test writes small CSV files into pytest's temporary folder (tmp_path),
so the real data/ folder is never read or modified.

Run from the project root:
    python -m pytest tests/test_extract.py -v
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

import etl

MOVIES_OK = (
    "movieId,title,genres\n"
    "1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy\n"
    '11,"American President, The (1995)",Comedy|Drama|Romance\n'
    "73,\"Mis\u00e9rables, Les (1995)\",Drama|War\n"
)
RATINGS_OK = (
    "userId,movieId,rating,timestamp\n"
    "1,1,4.0,964982703\n"
    "1,11,5.0,964981247\n"
    "2,73,0.5,1537799250\n"
)
LINKS_OK = (
    "movieId,imdbId,tmdbId\n"
    "1,0114709,862\n"
    "11,0112346,9087\n"
    "73,0113828,\n"
)


def write_files(folder: Path, movies: str = MOVIES_OK, ratings: str = RATINGS_OK,
                links: str = LINKS_OK) -> Path:
    """Write the three CSVs (UTF-8) into `folder`; pass None to skip a file."""
    for name, text in (("movies.csv", movies), ("ratings.csv", ratings), ("links.csv", links)):
        if text is not None:
            (folder / name).write_text(text, encoding="utf-8")
    return folder


# ------------------------------------------------------------- valid input

def test_valid_files_are_extracted_with_correct_types(tmp_path):
    data = etl.extract_csv_sources(write_files(tmp_path))

    assert len(data.movies) == 3 and len(data.ratings) == 3 and len(data.links) == 3
    assert str(data.movies["movieId"].dtype) == "int64"
    assert str(data.ratings["userId"].dtype) == "int64"
    assert str(data.ratings["timestamp"].dtype) == "int64"
    assert data.ratings["rating"].tolist() == [4.0, 5.0, 0.5]


def test_titles_with_commas_and_accents_are_preserved(tmp_path):
    movies = etl.extract_movies(write_files(tmp_path))
    titles = set(movies["title"])
    assert "American President, The (1995)" in titles
    assert "Mis\u00e9rables, Les (1995)" in titles  # UTF-8 read correctly


def test_imdb_id_keeps_leading_zeros(tmp_path):
    links = etl.extract_links(write_files(tmp_path))
    assert links.loc[links["movieId"] == 1, "imdb_id"].item() == "tt0114709"


@pytest.mark.parametrize("raw, expected", [
    ("0114709", "tt0114709"),
    ("114709", "tt0114709"),      # leading zero lost by a spreadsheet -> restored
    ("tt0114709", "tt0114709"),   # already prefixed
    ("12345678", "tt12345678"),   # newer 8-digit IMDb IDs
    ("abc", None),
    (None, None),
])
def test_format_imdb_id(raw, expected):
    assert etl.format_imdb_id(raw) == expected


def test_extra_columns_are_ignored(tmp_path):
    movies = "movieId,title,genres,notes\n1,Toy Story (1995),Comedy,extra\n"
    df = etl.extract_movies(write_files(tmp_path, movies=movies))
    assert list(df.columns) == ["movieId", "title", "genres"]


# ------------------------------------------------------------- missing / bad files

def test_missing_file_raises_clear_error(tmp_path):
    write_files(tmp_path, ratings=None)
    with pytest.raises(FileNotFoundError, match="ratings.csv"):
        etl.extract_csv_sources(tmp_path)


def test_missing_required_column_raises(tmp_path):
    bad = "movieId,name,genres\n1,Toy Story (1995),Comedy\n"  # 'title' renamed
    with pytest.raises(etl.DataValidationError, match="title"):
        etl.extract_movies(write_files(tmp_path, movies=bad))


def test_header_only_file_raises(tmp_path):
    with pytest.raises(etl.DataValidationError, match="no valid"):
        etl.extract_movies(write_files(tmp_path, movies="movieId,title,genres\n"))


# ------------------------------------------------------------- bad rows are dropped, not fatal

def test_invalid_movie_rows_dropped(tmp_path):
    movies = (
        "movieId,title,genres\n"
        "1,Toy Story (1995),Comedy\n"
        "abc,Bad Id (2000),Drama\n"        # non-integer id
        "2.5,Fractional Id (2000),Drama\n" # non-integer id
        "3,,Drama\n"                        # missing title
        "4,   Padded Title (1999)   ,\n"    # padded title, missing genres -> kept
    )
    df = etl.extract_movies(write_files(tmp_path, movies=movies))
    assert df["movieId"].tolist() == [1, 4]
    assert df.loc[df["movieId"] == 4, "title"].item() == "Padded Title (1999)"
    assert df.loc[df["movieId"] == 4, "genres"].isna().item()


def test_title_text_na_is_not_treated_as_missing(tmp_path):
    movies = "movieId,title,genres\n1,NA,Drama\n"
    df = etl.extract_movies(write_files(tmp_path, movies=movies))
    assert df["title"].item() == "NA"


def test_duplicate_movie_id_keeps_first(tmp_path):
    movies = "movieId,title,genres\n1,First (1995),Comedy\n1,Second (1996),Drama\n"
    df = etl.extract_movies(write_files(tmp_path, movies=movies))
    assert df["title"].tolist() == ["First (1995)"]


@pytest.mark.parametrize("bad_row", [
    "x,1,4.0,964982703",      # non-integer userId
    "0,1,4.0,964982703",      # userId must be positive
    "3,1,five,964982703",     # non-numeric rating
    "3,1,7.0,964982703",      # out of range
    "3,1,3.3,964982703",      # not a half step
    "3,1,,964982703",         # missing rating
    "3,1,4.0,",               # missing timestamp
    "3,1,4.0,yesterday",      # non-integer timestamp
])
def test_invalid_rating_rows_dropped(tmp_path, bad_row):
    ratings = RATINGS_OK + bad_row + "\n"
    df = etl.extract_ratings(write_files(tmp_path, ratings=ratings))
    assert len(df) == 3  # only the three valid rows remain


def test_duplicate_rating_keeps_most_recent(tmp_path):
    ratings = ("userId,movieId,rating,timestamp\n"
               "1,1,2.0,1000\n"
               "1,1,4.5,2000\n")
    df = etl.extract_ratings(write_files(tmp_path, ratings=ratings))
    assert len(df) == 1 and df["rating"].item() == 4.5


def test_ratings_for_unknown_movies_are_removed(tmp_path):
    ratings = RATINGS_OK + "5,999,3.0,964982703\n"  # movie 999 is not in movies.csv
    data = etl.extract_csv_sources(write_files(tmp_path, ratings=ratings))
    assert 999 not in set(data.ratings["movieId"])
    assert len(data.ratings) == 3


def test_links_without_tmdb_column_are_accepted(tmp_path):
    links = "movieId,imdbId\n1,0114709\n"
    df = etl.extract_links(write_files(tmp_path, links=links))
    assert df["imdb_id"].tolist() == ["tt0114709"]


def test_shared_imdb_id_is_kept_only_once(tmp_path):
    links = "movieId,imdbId\n1,0114709\n2,0114709\n"
    df = etl.extract_links(write_files(tmp_path, links=links))
    assert df.loc[df["movieId"] == 1, "imdb_id"].item() == "tt0114709"
    assert df.loc[df["movieId"] == 2, "imdb_id"].isna().item()


# ------------------------------------------------------------- configuration

def make_args(**overrides) -> argparse.Namespace:
    values = dict(data_dir=Path("data"), db=Path("movies.db"), log_dir=Path("logs"),
                  max_api_calls=None, skip_api=False, log_level="INFO")
    values.update(overrides)
    return argparse.Namespace(**values)


def test_config_reads_environment(monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "abcd1234")
    monkeypatch.setenv("OMDB_MAX_API_CALLS", "100")
    config = etl.load_config(make_args())
    assert config.omdb_api_key == "abcd1234"
    assert config.max_api_calls == 100


def test_command_line_overrides_environment(monkeypatch):
    monkeypatch.setenv("OMDB_MAX_API_CALLS", "100")
    assert etl.load_config(make_args(max_api_calls=5)).max_api_calls == 5


def test_invalid_max_api_calls_rejected(monkeypatch):
    monkeypatch.setenv("OMDB_MAX_API_CALLS", "lots")
    with pytest.raises(ValueError, match="OMDB_MAX_API_CALLS"):
        etl.load_config(make_args())


def test_placeholder_key_is_treated_as_missing(monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "your_omdb_api_key_here")
    assert etl.load_config(make_args()).omdb_api_key is None


def test_api_key_never_appears_in_config_repr(monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "secret99")
    assert "secret99" not in repr(etl.load_config(make_args()))


# ------------------------------------------------------------- end-to-end entry point

def test_main_returns_error_code_when_file_missing(tmp_path, monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "abcd1234")
    write_files(tmp_path, movies=None)
    code = etl.main(["--data-dir", str(tmp_path), "--log-dir", str(tmp_path / "logs")])
    assert code == 1


def test_main_succeeds_and_writes_utf8_log(tmp_path, monkeypatch):
    monkeypatch.setenv("OMDB_API_KEY", "abcd1234")
    write_files(tmp_path)
    log_dir = tmp_path / "logs"
    assert etl.main(["--data-dir", str(tmp_path), "--log-dir", str(log_dir)]) == 0

    log_text = (log_dir / "pipeline.log").read_text(encoding="utf-8")
    assert "Extracted 3 movies" in log_text
    assert "abcd1234" not in log_text  # the API key is never logged