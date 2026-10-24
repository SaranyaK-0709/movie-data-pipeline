"""
Data profiling for the MovieLens ml-latest-small files.

Purpose
-------
Measure what the raw CSV files actually contain *before* designing the
database schema and the ETL logic. Nothing here modifies any data; the
script only reads the files and prints a report.

Usage (from the project root, with the virtual environment active):
    python scripts/profile_data.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# A MovieLens title normally ends with the release year in parentheses,
# e.g. "Toy Story (1995)". This pattern captures that trailing year.
YEAR_AT_END = re.compile(r"\((\d{4})\)\s*$")

# Titles where a leading article was moved to the end,
# e.g. "American President, The (1995)".
TRAILING_ARTICLE = re.compile(r",\s(?:The|A|An|Les|La|Le|L'|Il|Das|Der|Die|El)\s*(?:\(|$)")


def section(title: str) -> None:
    """Print a visible section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def show_examples(series: pd.Series, limit: int = 10) -> None:
    """Print up to `limit` example values, one per line."""
    for value in series.head(limit).tolist():
        print(f"    {value!r}")
    if len(series) > limit:
        print(f"    ... and {len(series) - limit} more")


def load_csv(name: str, dtypes: dict[str, str]) -> pd.DataFrame:
    """Read one CSV from the data folder, exiting with a clear message if missing."""
    path = DATA_DIR / name
    if not path.exists():
        sys.exit(f"ERROR: {path} not found. Download the dataset first (see README).")
    # keep_default_na=False + explicit na_values: only truly empty cells become
    # missing. Otherwise pandas would turn strings such as "NA" or "None" into NaN.
    return pd.read_csv(path, dtype=dtypes, encoding="utf-8", keep_default_na=False, na_values=[""])


def profile_movies(movies: pd.DataFrame) -> None:
    section("movies.csv")
    print(f"Rows: {len(movies):,}")
    print(f"Columns: {list(movies.columns)}")
    print(f"Missing values per column:\n{movies.isna().sum().to_string()}")
    print(f"Unique movieId: {movies['movieId'].nunique():,}")
    print(f"Duplicate movieId rows: {movies['movieId'].duplicated().sum()}")

    # --- Titles ---------------------------------------------------------
    titles = movies["title"]
    print(f"\nTitles with leading/trailing whitespace: {(titles != titles.str.strip()).sum()}")

    dup_titles = movies[titles.duplicated(keep=False)].sort_values("title")
    print(f"Exact duplicate titles (rows involved): {len(dup_titles)}")
    if len(dup_titles):
        print(dup_titles[["movieId", "title", "genres"]].to_string(index=False))

    years = titles.str.extract(YEAR_AT_END)[0]
    no_year = movies[years.isna()]
    print(f"\nTitles WITHOUT a trailing (YYYY) year: {len(no_year)}")
    show_examples(no_year["title"], limit=15)

    years_num = pd.to_numeric(years, errors="coerce")
    print(f"Release year range (from titles): {int(years_num.min())} - {int(years_num.max())}")

    print(f"\nTitles with a moved article (e.g. ', The'): {titles.str.contains(TRAILING_ARTICLE).sum()}")
    show_examples(titles[titles.str.contains(TRAILING_ARTICLE)], limit=5)

    aka = titles[titles.str.contains(r"a\.k\.a\.", case=False, regex=True)]
    print(f"\nTitles containing 'a.k.a.': {len(aka)}")
    show_examples(aka, limit=5)

    # Titles with more than one parenthesised group, e.g. "Seven (a.k.a. Se7en) (1995)"
    multi_paren = titles[titles.str.count(r"\(") > 1]
    print(f"\nTitles with more than one '(' group: {len(multi_paren)}")
    show_examples(multi_paren, limit=8)

    non_ascii = titles[titles.map(lambda t: not t.isascii())]
    print(f"\nTitles with non-ASCII characters: {len(non_ascii)}")
    show_examples(non_ascii, limit=5)

    # --- Genres ---------------------------------------------------------
    genre_lists = movies["genres"].str.split("|")
    exploded = genre_lists.explode()
    print(f"\nDistinct genre values: {exploded.nunique()}")
    print(exploded.value_counts().to_string())
    per_movie = genre_lists.str.len()
    print(f"\nGenres per movie: min={per_movie.min()}, max={per_movie.max()}, "
          f"mean={per_movie.mean():.2f}")
    print(f"Total movie-genre pairs: {len(exploded):,}")


def profile_ratings(ratings: pd.DataFrame, movies: pd.DataFrame) -> None:
    section("ratings.csv")
    print(f"Rows: {len(ratings):,}")
    print(f"Columns: {list(ratings.columns)}")
    print(f"Missing values per column:\n{ratings.isna().sum().to_string()}")
    print(f"Distinct users: {ratings['userId'].nunique():,}")
    print(f"Distinct movies rated: {ratings['movieId'].nunique():,}")

    dup_pairs = ratings.duplicated(subset=["userId", "movieId"]).sum()
    print(f"Duplicate (userId, movieId) pairs: {dup_pairs}")

    print(f"\nRating min/max: {ratings['rating'].min()} / {ratings['rating'].max()}")
    print("Rating value counts:")
    print(ratings["rating"].value_counts().sort_index().to_string())

    ts = pd.to_datetime(ratings["timestamp"], unit="s", utc=True)
    print(f"\nTimestamp range (UTC): {ts.min()}  ->  {ts.max()}")

    # --- Referential integrity between files ------------------------------
    orphan = ~ratings["movieId"].isin(movies["movieId"])
    print(f"\nRatings whose movieId is NOT in movies.csv: {orphan.sum()}")
    unrated = ~movies["movieId"].isin(ratings["movieId"])
    print(f"Movies in movies.csv with NO ratings: {unrated.sum()}")

    # --- Distribution: why Q1 needs care ---------------------------------
    counts = ratings.groupby("movieId").size()
    print("\nRatings per movie:")
    for n in (1, 2, 5, 10, 20, 50, 100):
        print(f"    movies with >= {n:>3} ratings: {(counts >= n).sum():,}")
    print(f"    movies with exactly 1 rating: {(counts == 1).sum():,}")

    avg = ratings.groupby("movieId")["rating"].mean()
    perfect = avg[avg == 5.0]
    print(f"\nMovies with a perfect 5.0 average: {len(perfect)}")
    print(f"    of those, how many have only 1 rating: {(counts[perfect.index] == 1).sum()}")

    # --- API budget planning: coverage if we enrich most-rated first -------
    sorted_counts = counts.sort_values(ascending=False)
    total = sorted_counts.sum()
    print("\nShare of all ratings covered if the N most-rated movies are enriched:")
    for n in (950, 1900, 2850, 5000):
        share = sorted_counts.head(n).sum() / total
        print(f"    top {n:>5,} movies -> {share:6.1%} of ratings")


def profile_links(links: pd.DataFrame, movies: pd.DataFrame) -> None:
    section("links.csv")
    print(f"Rows: {len(links):,}")
    print(f"Columns: {list(links.columns)}")
    print(f"Missing values per column:\n{links.isna().sum().to_string()}")

    imdb = links["imdbId"].dropna()
    print(f"\nimdbId string lengths (leading zeros kept): "
          f"{imdb.str.len().value_counts().sort_index().to_dict()}")
    non_numeric = (~imdb.str.fullmatch(r"\d+")).sum()
    print(f"Non-numeric imdbId values: {non_numeric}")

    dup_imdb = links[links["imdbId"].notna() & links["imdbId"].duplicated(keep=False)]
    print(f"Rows sharing an imdbId with another movie: {len(dup_imdb)}")
    if len(dup_imdb):
        merged = dup_imdb.merge(movies, on="movieId")[["movieId", "imdbId", "title"]]
        print(merged.sort_values("imdbId").to_string(index=False))

    missing_link = ~movies["movieId"].isin(links["movieId"])
    print(f"\nMovies with no row in links.csv: {missing_link.sum()}")
    extra_link = ~links["movieId"].isin(movies["movieId"])
    print(f"links.csv rows whose movieId is not in movies.csv: {extra_link.sum()}")


def main() -> None:
    print(f"pandas {pd.__version__} | data folder: {DATA_DIR}")
    # Read IDs as strings first where formatting matters (imdbId has leading zeros).
    movies = load_csv("movies.csv", {"movieId": "int64", "title": "str", "genres": "str"})
    ratings = load_csv("ratings.csv", {"userId": "int64", "movieId": "int64",
                                       "rating": "float64", "timestamp": "int64"})
    links = load_csv("links.csv", {"movieId": "int64", "imdbId": "str", "tmdbId": "str"})

    profile_movies(movies)
    profile_ratings(ratings, movies)
    profile_links(links, movies)
    section("Profiling complete")


if __name__ == "__main__":
    main()
