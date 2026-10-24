"""
etl.py - Movie Data Pipeline (MovieLens + OMDb -> SQLite)

Pipeline stages
    1. Extract   read and validate movies.csv, ratings.csv, links.csv
    2. Enrich    fetch movie details from the OMDb API (cached, resumable)   [Sub-step 4.2]
    3. Transform clean, type and reshape the data                            [Sub-step 4.3]
    4. Load      upsert into SQLite inside one transaction (idempotent)      [Sub-step 4.4]

Usage (from the project root, virtual environment active):
    python etl.py                      # run the pipeline with defaults
    python etl.py --help               # show all options

Configuration comes from command-line options, then environment variables /
the .env file (see .env.example). The OMDb API key is only ever read from the
environment and is never logged.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DB_PATH = PROJECT_ROOT / "movies.db"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logs"
SCHEMA_PATH = PROJECT_ROOT / "schema.sql"

DEFAULT_MAX_API_CALLS = 950

# Columns each input file must contain. Extra columns are allowed and ignored.
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "movies.csv": ["movieId", "title", "genres"],
    "ratings.csv": ["userId", "movieId", "rating", "timestamp"],
    "links.csv": ["movieId", "imdbId"],
}

VALID_RATINGS = {x / 2 for x in range(1, 11)}  # 0.5, 1.0, ..., 5.0

logger = logging.getLogger("etl")


class DataValidationError(Exception):
    """Raised when an input file cannot be used (missing columns, no valid rows)."""


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """All settings for one pipeline run."""

    data_dir: Path
    db_path: Path
    log_dir: Path
    max_api_calls: int
    skip_api: bool
    # repr=False keeps the key out of logs and error messages if Config is printed.
    omdb_api_key: str | None = field(default=None, repr=False)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Define and parse the command-line options."""
    parser = argparse.ArgumentParser(description="MovieLens + OMDb -> SQLite ETL pipeline")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR,
                        help="folder containing movies.csv, ratings.csv, links.csv")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                        help="SQLite database file to create/update")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR,
                        help="folder for the pipeline log file")
    parser.add_argument("--max-api-calls", type=int, default=None,
                        help="maximum OMDb requests this run (default: OMDB_MAX_API_CALLS or "
                             f"{DEFAULT_MAX_API_CALLS})")
    parser.add_argument("--skip-api", action="store_true",
                        help="do not call OMDb; use only previously cached responses")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args(argv)


def load_config(args: argparse.Namespace) -> Config:
    """Combine command-line options with environment variables (.env)."""
    load_dotenv(PROJECT_ROOT / ".env")

    max_calls = args.max_api_calls
    if max_calls is None:
        raw = os.getenv("OMDB_MAX_API_CALLS", str(DEFAULT_MAX_API_CALLS))
        try:
            max_calls = int(raw)
        except ValueError as exc:
            raise ValueError(f"OMDB_MAX_API_CALLS must be an integer, got {raw!r}") from exc
    if max_calls < 0:
        raise ValueError(f"max API calls cannot be negative, got {max_calls}")

    key = (os.getenv("OMDB_API_KEY") or "").strip() or None
    if key == "your_omdb_api_key_here":  # the unedited placeholder from .env.example
        key = None

    return Config(
        data_dir=args.data_dir,
        db_path=args.db,
        log_dir=args.log_dir,
        max_api_calls=max_calls,
        skip_api=args.skip_api,
        omdb_api_key=key,
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path, level: str = "INFO") -> Path:
    """Log to the console and to logs/pipeline.log (UTF-8, appended each run)."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "pipeline.log"

    # If output is redirected on Windows, the console encoding may not support
    # accented characters; replace them instead of crashing.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    formatter = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                                  datefmt="%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")  # explicit UTF-8
    file_handler.setFormatter(formatter)

    for old in list(logger.handlers):  # close handlers from a previous call (frees the file)
        old.close()
        logger.removeHandler(old)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.setLevel(level)
    logger.propagate = False
    return log_file


# ---------------------------------------------------------------------------
# Extract: CSV files
# ---------------------------------------------------------------------------

def read_csv_checked(path: Path, required_columns: list[str]) -> pd.DataFrame:
    """
    Read a CSV with every column as text, after checking the file and its header.

    Reading as text first means a bad value (e.g. 'abc' in an ID column) can be
    found and reported row by row, instead of making pandas fail on the whole file.
    """
    if not path.is_file():
        raise FileNotFoundError(
            f"Input file not found: {path}. Download the MovieLens 'ml-latest-small' "
            f"dataset and place the CSV files in {path.parent} (see README)."
        )

    # keep_default_na=False: only empty cells count as missing; a title such as
    # "NA" or "None" stays text instead of silently becoming NaN.
    df = pd.read_csv(path, dtype=str, encoding="utf-8",
                     keep_default_na=False, na_values=[""])

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise DataValidationError(
            f"{path.name} is missing required column(s) {missing}; found {list(df.columns)}"
        )
    return df[required_columns].copy()


def _to_int(series: pd.Series) -> pd.Series:
    """Convert text to nullable integers; non-integers (e.g. 'abc', '1.5') become <NA>."""
    numbers = pd.to_numeric(series.str.strip(), errors="coerce")
    whole = numbers.notna() & (numbers % 1 == 0)
    return numbers.where(whole).astype("Int64")


def _drop_invalid(df: pd.DataFrame, valid: pd.Series, raw: pd.DataFrame,
                  file_name: str, reason: str) -> pd.DataFrame:
    """
    Remove rows where `valid` is False, logging how many and a few examples.

    Examples are taken from `raw` (the file as read) so the log shows the
    original bad value, e.g. 'abc', rather than its converted form (<NA>).
    """
    bad_index = df.index[~valid]
    if len(bad_index):
        examples = raw.loc[bad_index[:3]].to_dict("records")
        logger.warning("%s: dropped %d row(s) with %s. Examples (as in file): %s",
                       file_name, len(bad_index), reason, examples)
    return df[valid]


def extract_movies(data_dir: Path) -> pd.DataFrame:
    """Read movies.csv -> columns movieId (int), title (str), genres (str or <NA>)."""
    name = "movies.csv"
    raw = read_csv_checked(data_dir / name, REQUIRED_COLUMNS[name])
    df = raw.assign(movieId=_to_int(raw["movieId"]), title=raw["title"].str.strip())

    df = _drop_invalid(df, df["movieId"].notna(), raw, name, "a missing or non-integer movieId")
    df = _drop_invalid(df, df["title"].notna() & (df["title"] != ""), raw, name, "a missing title")

    dupes = df["movieId"].duplicated(keep="first")
    df = _drop_invalid(df, ~dupes, raw, name, "a duplicate movieId (first occurrence kept)")

    if df.empty:
        raise DataValidationError(f"{name} contains no valid movie rows")
    df = df.astype({"movieId": "int64"}).reset_index(drop=True)
    logger.info("Extracted %d movies from %s (%d raw rows)", len(df), name, len(raw))
    return df


def extract_ratings(data_dir: Path) -> pd.DataFrame:
    """Read ratings.csv -> userId (int), movieId (int), rating (float), timestamp (int)."""
    name = "ratings.csv"
    raw = read_csv_checked(data_dir / name, REQUIRED_COLUMNS[name])
    df = raw.assign(
        userId=_to_int(raw["userId"]),
        movieId=_to_int(raw["movieId"]),
        rating=pd.to_numeric(raw["rating"].str.strip(), errors="coerce"),
        timestamp=_to_int(raw["timestamp"]),
    )

    ids_ok = df["userId"].notna() & (df["userId"] > 0) & df["movieId"].notna()
    df = _drop_invalid(df, ids_ok, raw, name, "a missing/invalid userId or movieId")
    df = _drop_invalid(df, df["rating"].isin(VALID_RATINGS), raw, name,
                       "a rating outside 0.5-5.0 in half steps")
    df = _drop_invalid(df, df["timestamp"].notna() & (df["timestamp"] >= 0), raw, name,
                       "a missing/invalid timestamp")

    # One rating per (user, movie): if the source repeats a pair, keep the most recent.
    df = df.sort_values("timestamp", kind="stable")
    dupes = df.duplicated(subset=["userId", "movieId"], keep="last")
    df = _drop_invalid(df, ~dupes, raw, name, "a duplicate (userId, movieId) pair (latest kept)")

    if df.empty:
        raise DataValidationError(f"{name} contains no valid rating rows")
    df = (df.astype({"userId": "int64", "movieId": "int64", "timestamp": "int64"})
            .sort_values(["userId", "movieId"]).reset_index(drop=True))
    logger.info("Extracted %d ratings from %s (%d raw rows)", len(df), name, len(raw))
    return df


def format_imdb_id(raw_id: str | None) -> str | None:
    """'114709' or '0114709' -> 'tt0114709'. Returns None for missing/non-numeric values."""
    if raw_id is None or pd.isna(raw_id):
        return None
    digits = str(raw_id).strip()
    if digits.lower().startswith("tt"):
        digits = digits[2:]
    if not digits.isdigit():
        return None
    return "tt" + digits.zfill(7)


def extract_links(data_dir: Path) -> pd.DataFrame:
    """Read links.csv -> movieId (int), imdb_id (str 'tt0114709' or <NA>)."""
    name = "links.csv"
    raw = read_csv_checked(data_dir / name, REQUIRED_COLUMNS[name])
    df = raw.assign(movieId=_to_int(raw["movieId"]),
                    imdb_id=raw["imdbId"].map(format_imdb_id))

    df = _drop_invalid(df, df["movieId"].notna(), raw, name, "a missing or non-integer movieId")
    df = _drop_invalid(df, ~df["movieId"].duplicated(keep="first"), raw, name,
                       "a duplicate movieId (first occurrence kept)")

    bad_imdb = df["imdb_id"].isna() & raw.loc[df.index, "imdbId"].notna()
    if bad_imdb.any():
        logger.warning("%s: %d imdbId value(s) are not numeric and were ignored",
                       name, int(bad_imdb.sum()))

    # Two movies must never share one IMDb ID (the database enforces UNIQUE).
    shared = df["imdb_id"].notna() & df["imdb_id"].duplicated(keep="first")
    if shared.any():
        logger.warning("%s: %d movie(s) share an IMDb ID with an earlier movie; "
                       "their IMDb ID was ignored", name, int(shared.sum()))
        df.loc[shared, "imdb_id"] = None

    df = df[["movieId", "imdb_id"]].astype({"movieId": "int64"}).reset_index(drop=True)
    logger.info("Extracted %d links from %s (%d with an IMDb ID)",
                len(df), name, int(df["imdb_id"].notna().sum()))
    return df


@dataclass
class ExtractedData:
    """The validated source data handed from Extract to the next stages."""

    movies: pd.DataFrame
    ratings: pd.DataFrame
    links: pd.DataFrame


def extract_csv_sources(data_dir: Path) -> ExtractedData:
    """Run all CSV extraction and report cross-file consistency."""
    movies = extract_movies(data_dir)
    ratings = extract_ratings(data_dir)
    links = extract_links(data_dir)

    known = set(movies["movieId"])
    orphan_ratings = ~ratings["movieId"].isin(known)
    if orphan_ratings.any():
        # The database would reject these (foreign key), so remove them here, visibly.
        logger.warning("ratings.csv: dropped %d rating(s) for movieIds not in movies.csv",
                       int(orphan_ratings.sum()))
        ratings = ratings[~orphan_ratings].reset_index(drop=True)

    unlinked = ~movies["movieId"].isin(set(links["movieId"][links["imdb_id"].notna()]))
    if unlinked.any():
        logger.warning("%d movie(s) have no IMDb ID; OMDb lookup will fall back to title + year",
                       int(unlinked.sum()))

    return ExtractedData(movies=movies, ratings=ratings, links=links)


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(config: Config) -> ExtractedData:
    """Run the pipeline stages in order. (Enrich/Transform/Load are added in 4.2-4.4.)"""
    logger.info("Pipeline started | data=%s | db=%s | max_api_calls=%d | skip_api=%s",
                config.data_dir, config.db_path, config.max_api_calls, config.skip_api)
    logger.info("OMDb API key configured: %s", "yes" if config.omdb_api_key else "NO")

    extracted = extract_csv_sources(config.data_dir)

    logger.info("Extract stage complete. Enrich, Transform and Load are not implemented yet.")
    return extracted


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point. Returns a process exit code (0 = success)."""
    args = parse_args(argv)
    try:
        config = load_config(args)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    log_file = setup_logging(config.log_dir, args.log_level)
    logger.info("Logging to %s", log_file)
    try:
        run_pipeline(config)
    except (FileNotFoundError, DataValidationError) as exc:
        logger.error("Pipeline stopped: %s", exc)
        return 1
    except Exception:
        logger.exception("Pipeline failed with an unexpected error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())