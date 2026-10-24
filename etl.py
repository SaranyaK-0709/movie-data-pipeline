"""
etl.py - Movie Data Pipeline (MovieLens + OMDb -> SQLite)

Pipeline stages
    1. Extract   read and validate movies.csv, ratings.csv, links.csv
    2. Enrich    fetch movie details from the OMDb API (cached, resumable)
    3. Transform clean, type and reshape the data
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
import json
import logging
import os
import re
import sqlite3
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
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

# OMDb API settings
OMDB_URL = "https://www.omdbapi.com/"
OMDB_TIMEOUT_SECONDS = 10
OMDB_MAX_ATTEMPTS = 3          # tries per request for temporary failures (timeouts, 5xx)
OMDB_BACKOFF_SECONDS = 1.0     # wait 1s, then 2s, ... between those tries
OMDB_DELAY_SECONDS = 0.1       # pause between movies, to be polite to the free service
MAX_ERROR_RUNS = 3             # a movie whose lookup keeps failing is retried on at most 3 runs
YEAR_TOLERANCE = 1             # title fallback accepts OMDb year within +/- 1 of MovieLens year

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
# Database connection
# ---------------------------------------------------------------------------

def connect_db(db_path: Path) -> sqlite3.Connection:
    """Open the SQLite database, enforce foreign keys and apply schema.sql."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")  # SQLite does NOT enforce FKs without this
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))  # IF NOT EXISTS: safe to repeat
    return conn


def utc_now() -> str:
    """Current UTC time as 'YYYY-MM-DD HH:MM:SS'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Enrich: OMDb API client
# ---------------------------------------------------------------------------

class OmdbFatalError(Exception):
    """A problem that makes every further request pointless (bad key, quota used up)."""


class OmdbAuthError(OmdbFatalError):
    """OMDb rejected the API key."""


class OmdbRateLimitError(OmdbFatalError):
    """OMDb reports the request limit has been reached."""


class ApiBudgetExhausted(Exception):
    """This run has used all the requests it is allowed (max_api_calls)."""


@dataclass
class OmdbResult:
    """Outcome of one logical OMDb lookup (which may have needed several HTTP tries)."""

    status: str                         # 'found' | 'not_found' | 'error'
    http_status: int | None = None
    data: dict[str, Any] | None = None  # parsed JSON body, if any
    error: str | None = None


class OmdbClient:
    """
    Small OMDb client with timeouts, retries, quota counting and error classification.

    Important OMDb behaviour (observed in testing):
      * "not found" comes back as HTTP 200 with {"Response": "False", "Error": "..."},
        so success is decided by the JSON body, not the status code;
      * an invalid key comes back as HTTP 401 {"Error": "Invalid API key!"}.
    """

    def __init__(self, api_key: str, max_calls: int, session: Any = None,
                 sleep: Callable[[float], None] = time.sleep,
                 timeout: float = OMDB_TIMEOUT_SECONDS, max_attempts: int = OMDB_MAX_ATTEMPTS,
                 backoff: float = OMDB_BACKOFF_SECONDS) -> None:
        self._api_key = api_key
        self.max_calls = max_calls
        self.calls_made = 0             # every HTTP request counts against the daily quota
        self.retries = 0                # requests repeated after a temporary failure
        self._session = session or requests.Session()
        self._sleep = sleep
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff = backoff

    def _redact(self, text: str) -> str:
        """Remove the API key from text (requests' error messages include the full URL)."""
        return text.replace(self._api_key, "***") if self._api_key else text

    def fetch(self, params: dict[str, str]) -> OmdbResult:
        """Run one lookup, retrying temporary failures. Raises on fatal/budget conditions."""
        last_error = "unknown error"
        last_status: int | None = None
        for attempt in range(1, self._max_attempts + 1):
            if self.calls_made >= self.max_calls:
                raise ApiBudgetExhausted(f"used all {self.max_calls} API calls for this run")
            self.calls_made += 1
            try:
                response = self._session.get(OMDB_URL, params={**params, "apikey": self._api_key},
                                             timeout=self._timeout)
            except requests.Timeout:
                last_error = f"timeout after {self._timeout}s"
            except requests.RequestException as exc:  # connection refused, DNS failure, ...
                last_error = self._redact(f"{type(exc).__name__}: {exc}")
            else:
                last_status = response.status_code
                result = self._classify(response)
                if result is not None:
                    return result
                last_error = f"HTTP {response.status_code} or unreadable response"

            if attempt < self._max_attempts:
                wait = self._backoff * (2 ** (attempt - 1))
                self.retries += 1
                logger.warning("OMDb request failed (%s); retry %d/%d in %.1fs",
                               last_error, attempt, self._max_attempts - 1, wait)
                self._sleep(wait)
        return OmdbResult("error", http_status=last_status, error=last_error)

    def _classify(self, response: Any) -> OmdbResult | None:
        """Turn an HTTP response into a result; None means 'temporary, retry'."""
        try:
            body = response.json()
        except ValueError:
            body = None
        error_text = body.get("Error", "") if isinstance(body, dict) else ""

        # Fatal conditions are recognised by the error text first, then by status code.
        if "invalid api key" in error_text.lower() or "no api key" in error_text.lower():
            raise OmdbAuthError(f"OMDb rejected the API key ({error_text})")
        if "limit" in error_text.lower() or response.status_code == 429:
            raise OmdbRateLimitError(f"OMDb request limit reached ({error_text or 'HTTP 429'})")
        if response.status_code == 401:
            raise OmdbAuthError(f"OMDb returned HTTP 401 ({error_text or 'no message'})")

        if response.status_code >= 500:
            return None                                   # server problem: retry
        if not isinstance(body, dict):
            return None                                   # malformed/empty body: retry
        if body.get("Response") == "True":
            return OmdbResult("found", response.status_code, body)
        if body.get("Response") == "False":
            if "not found" in error_text.lower() or "incorrect imdb id" in error_text.lower():
                return OmdbResult("not_found", response.status_code, body, error_text)
            return OmdbResult("error", response.status_code, body, error_text or "unknown OMDb error")
        return OmdbResult("error", response.status_code, body, "response has no 'Response' field")


# ---------------------------------------------------------------------------
# Enrich: matching a MovieLens movie to OMDb
# ---------------------------------------------------------------------------

TITLE_YEAR = re.compile(r"\((\d{4})[^)]*\)\s*$")       # "(1995)" or "(2006-2007)" at the end
TRAILING_PARENS = re.compile(r"\s*\([^()]*\)\s*$")
MOVED_ARTICLE = re.compile(r"^(?P<rest>.+), (?P<article>The|A|An|Les|La|Le|L'|Il|El|Das|Der|Die)$")


def year_from_title(title: str) -> int | None:
    """'Toy Story (1995)' -> 1995; a range like '(2006-2007)' -> 2006; 'Black Mirror' -> None."""
    match = TITLE_YEAR.search(title)
    return int(match.group(1)) if match else None


def parse_omdb_year(value: Any) -> int | None:
    """OMDb 'Year' such as '1995', '2006-2007' or 'N/A' -> the first 4-digit year, or None."""
    match = re.match(r"\s*(\d{4})", str(value or ""))
    return int(match.group(1)) if match else None


def search_title(title: str) -> str:
    """
    Turn a MovieLens title into the form OMDb expects for a title search.

    'American President, The (1995)'                     -> 'The American President'
    'Seven (a.k.a. Se7en) (1995)'                        -> 'Seven'
    'Postman, The (Postino, Il) (1994)'                  -> 'The Postman'
    """
    text = title.strip()
    while TRAILING_PARENS.search(text):                 # drop year and alternate titles
        text = TRAILING_PARENS.sub("", text)
    match = MOVED_ARTICLE.match(text)
    if match:
        article = match.group("article")
        sep = "" if article.endswith("'") else " "
        text = f"{article}{sep}{match.group('rest')}"
    return text.strip()


@dataclass
class CacheEntry:
    """What gets written to omdb_cache for one movie."""

    movie_id: int
    imdb_id: str | None
    status: str
    lookup_method: str
    http_status: int | None = None
    response_json: str | None = None
    error_message: str | None = None


def lookup_movie(client: OmdbClient, movie_id: int, title: str,
                 imdb_id: str | None) -> CacheEntry:
    """
    Find one movie in OMDb.

    1. By IMDb ID (exact; preferred).
    2. Only if that is impossible or not found: by cleaned title + year, accepting
       the result only when OMDb's year is within YEAR_TOLERANCE of the MovieLens year.
       Without a year, no title search is made: it could attach the wrong film.
    """
    def entry(result: OmdbResult, method: str, error: str | None = None) -> CacheEntry:
        body = json.dumps(result.data, ensure_ascii=False) if result.data is not None else None
        return CacheEntry(movie_id, imdb_id, result.status, method, result.http_status,
                          body, error or result.error)

    first: OmdbResult | None = None
    if imdb_id:
        # No 'type' filter: the ID is exact, and some MovieLens entries are TV series.
        first = client.fetch({"i": imdb_id})
        if first.status in ("found", "error"):
            return entry(first, "imdb_id")

    year = year_from_title(title)
    if year is None:
        reason = "not found by IMDb ID" if imdb_id else "no IMDb ID"
        result = first or OmdbResult("not_found")
        return entry(result, "imdb_id" if imdb_id else "none",
                     f"{reason}; no release year, so no title search (risk of a wrong match)")

    result = client.fetch({"t": search_title(title), "y": str(year), "type": "movie"})
    if result.status != "found":
        return entry(result, "title_year")

    omdb_year = parse_omdb_year(result.data.get("Year") if result.data else None)
    if omdb_year is None or abs(omdb_year - year) > YEAR_TOLERANCE:
        return entry(OmdbResult("not_found", result.http_status, result.data), "title_year",
                     f"title match rejected: OMDb year {omdb_year} vs MovieLens year {year}")
    return entry(result, "title_year")


# ---------------------------------------------------------------------------
# Enrich: cache + resumable stage
# ---------------------------------------------------------------------------

def save_cache_entry(conn: sqlite3.Connection, e: CacheEntry) -> None:
    """Insert or update one movie's cache row and commit immediately (crash-safe progress)."""
    conn.execute(
        """
        INSERT INTO omdb_cache (movie_id, imdb_id, status, lookup_method, http_status,
                                response_json, error_message, attempts, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT (movie_id) DO UPDATE SET
            imdb_id = excluded.imdb_id, status = excluded.status,
            lookup_method = excluded.lookup_method, http_status = excluded.http_status,
            response_json = excluded.response_json, error_message = excluded.error_message,
            attempts = omdb_cache.attempts + 1, fetched_at = excluded.fetched_at
        """,
        (e.movie_id, e.imdb_id, e.status, e.lookup_method, e.http_status,
         e.response_json, e.error_message, utc_now()),
    )
    conn.commit()


def movies_needing_lookup(conn: sqlite3.Connection, data: ExtractedData) -> pd.DataFrame:
    """
    Movies with no final cache result, most-rated first.

    'found' and 'not_found' are final. 'error' is retried on later runs, up to
    MAX_ERROR_RUNS attempts in total, so one broken record cannot use the quota forever.
    """
    cached = pd.read_sql_query("SELECT movie_id, status, attempts FROM omdb_cache", conn)
    done = cached.loc[(cached["status"] != "error") | (cached["attempts"] >= MAX_ERROR_RUNS),
                      "movie_id"]
    rating_counts = data.ratings.groupby("movieId").size().rename("n_ratings")
    todo = (data.movies.merge(data.links, on="movieId", how="left")
            .merge(rating_counts, left_on="movieId", right_index=True, how="left")
            .fillna({"n_ratings": 0}))
    todo = todo[~todo["movieId"].isin(set(done))]
    return todo.sort_values(["n_ratings", "movieId"], ascending=[False, True])


@dataclass
class EnrichStats:
    attempted: int = 0
    found: int = 0
    not_found: int = 0
    errors: int = 0
    api_calls: int = 0
    retries: int = 0
    remaining: int = 0
    stopped_reason: str = "completed"


def enrich_with_omdb(conn: sqlite3.Connection, data: ExtractedData, config: Config,
                     session: Any = None, sleep: Callable[[float], None] = time.sleep) -> EnrichStats:
    """Look up every movie that has no final cached result, within this run's API budget."""
    stats = EnrichStats()
    todo = movies_needing_lookup(conn, data)
    stats.remaining = len(todo)

    if config.skip_api:
        stats.stopped_reason = "skipped (--skip-api)"
    elif not config.omdb_api_key:
        stats.stopped_reason = "skipped (no OMDB_API_KEY configured)"
        logger.warning("No OMDb API key configured; enrichment skipped, cached data will be used")
    elif config.max_api_calls == 0:
        stats.stopped_reason = "skipped (max_api_calls = 0)"
    elif todo.empty:
        stats.stopped_reason = "nothing to do (all movies already cached)"
    else:
        logger.info("Enrich: %d movie(s) need an OMDb lookup; budget %d API call(s)",
                    len(todo), config.max_api_calls)
        client = OmdbClient(config.omdb_api_key, config.max_api_calls, session=session, sleep=sleep)
        try:
            for i, row in enumerate(todo.itertuples(index=False), start=1):
                imdb_id = row.imdb_id if isinstance(row.imdb_id, str) else None
                e = lookup_movie(client, int(row.movieId), row.title, imdb_id)
                save_cache_entry(conn, e)
                stats.attempted += 1
                stats.remaining -= 1
                if e.status == "found":
                    stats.found += 1
                elif e.status == "not_found":
                    stats.not_found += 1
                else:
                    stats.errors += 1
                    logger.warning("OMDb error for movieId %s (%s): %s",
                                   row.movieId, row.title, e.error_message)
                if i % 100 == 0:
                    logger.info("Enrich progress: %d looked up, %d API calls used",
                                i, client.calls_made)
                sleep(OMDB_DELAY_SECONDS)
        except ApiBudgetExhausted as exc:
            stats.stopped_reason = f"API budget reached ({exc})"
        except OmdbFatalError as exc:
            stats.stopped_reason = f"stopped: {exc}"
            logger.error("Enrichment stopped early: %s. Progress so far is saved.", exc)
        except KeyboardInterrupt:
            logger.warning("Enrichment interrupted by user. Progress so far is saved.")
            raise
        finally:
            stats.api_calls = client.calls_made
            stats.retries = client.retries

    logger.info("Enrich: %s | looked up %d (found %d, not found %d, errors %d) | "
                "API calls %d (retries %d) | still to look up %d",
                stats.stopped_reason, stats.attempted, stats.found, stats.not_found,
                stats.errors, stats.api_calls, stats.retries, stats.remaining)
    log_cache_summary(conn, len(data.movies))
    return stats


def log_cache_summary(conn: sqlite3.Connection, total_movies: int) -> None:
    """Log overall enrichment coverage across all runs so far."""
    counts = dict(conn.execute("SELECT status, COUNT(*) FROM omdb_cache GROUP BY status").fetchall())
    found = counts.get("found", 0)
    logger.info("OMDb cache: %d found, %d not found, %d error, %d never looked up | "
                "coverage %.1f%% of %d movies",
                found, counts.get("not_found", 0), counts.get("error", 0),
                total_movies - sum(counts.values()), 100 * found / max(total_movies, 1),
                total_movies)


# ---------------------------------------------------------------------------
# Transform: value parsers (each turns one raw value into a clean typed value)
# ---------------------------------------------------------------------------

NO_GENRES = "(no genres listed)"
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}
MIN_YEAR, MAX_YEAR = 1870, 2100


def clean_text(value: Any) -> str | None:
    """Strip text; OMDb's 'N/A', empty strings and missing values become None."""
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    text = str(value).strip()
    return None if text in ("", "N/A") else text


def parse_box_office(value: Any) -> int | None:
    """'$292,587,330' -> 292587330; 'N/A' or anything unparseable -> None."""
    text = clean_text(value)
    if text is None:
        return None
    digits = text.replace("$", "").replace(",", "").strip()
    return int(digits) if digits.isdigit() else None


def parse_released_date(value: Any) -> str | None:
    """'16 Jul 2010' -> '2010-07-16' (ISO); 'N/A' or invalid dates -> None."""
    text = clean_text(value)
    match = re.fullmatch(r"(\d{1,2}) ([A-Z][a-z]{2}) (\d{4})", text or "")
    if not match or match.group(2) not in MONTHS:
        return None
    day, month, year = int(match.group(1)), MONTHS[match.group(2)], int(match.group(3))
    try:  # rejects impossible dates such as 31 Feb
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_runtime(value: Any) -> int | None:
    """'148 min' -> 148; 'N/A', '0 min' or other formats -> None."""
    match = re.fullmatch(r"(\d+)\s*min", clean_text(value) or "")
    minutes = int(match.group(1)) if match else 0
    return minutes if minutes > 0 else None


def split_directors(value: Any) -> list[str]:
    """
    'Lana Wachowski, Lilly Wachowski' -> ['Lana Wachowski', 'Lilly Wachowski'].
    Notes such as '(uncredited)' are removed; duplicates and 'N/A' are dropped.
    """
    names: list[str] = []
    for part in (clean_text(value) or "").split(","):
        name = re.sub(r"\s*\([^)]*\)\s*$", "", part).strip()
        if name and name != "N/A" and name not in names:
            names.append(name)
    return names


def split_genres(value: Any) -> list[str]:
    """'Adventure|Animation' -> ['Adventure', 'Animation']; '(no genres listed)' -> []."""
    genres: list[str] = []
    for part in (clean_text(value) or "").split("|"):
        genre = part.strip()
        if genre and genre != NO_GENRES and genre not in genres:
            genres.append(genre)
    return genres


def valid_year(year: int | None) -> int | None:
    """Keep only plausible release years (matches the CHECK in schema.sql)."""
    return year if year is not None and MIN_YEAR <= year <= MAX_YEAR else None


def decade_of(year: int | None) -> int | None:
    """Feature engineering: 1995 -> 1990."""
    return None if year is None else (year // 10) * 10


def unix_to_utc_text(seconds: pd.Series) -> pd.Series:
    """Unix epoch seconds -> 'YYYY-MM-DD HH:MM:SS' in UTC."""
    return pd.to_datetime(seconds, unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Transform: build the rows for each table
# ---------------------------------------------------------------------------

@dataclass
class TransformedData:
    """Clean rows ready for loading. Genre/director IDs are assigned by the database."""

    movies: pd.DataFrame        # one row per movie, columns match the movies table
    movie_genres: pd.DataFrame  # movie_id, genre_name
    movie_directors: pd.DataFrame  # movie_id, director_name
    ratings: pd.DataFrame       # user_id, movie_id, rating, rated_at


def read_cache(conn: sqlite3.Connection) -> pd.DataFrame:
    """All cached OMDb results: movie_id, status, response_json, fetched_at."""
    return pd.read_sql_query(
        "SELECT movie_id, status, response_json, fetched_at FROM omdb_cache", conn)


def _parse_json(text: Any, movie_id: int) -> dict[str, Any]:
    """Parse a cached response body; a corrupt body is logged and treated as empty."""
    if not isinstance(text, str):
        return {}
    try:
        body = json.loads(text)
    except ValueError:
        logger.warning("Cached OMDb response for movieId %s is not valid JSON; ignored", movie_id)
        return {}
    return body if isinstance(body, dict) else {}


STATUS_FROM_CACHE = {"found": "enriched", "not_found": "not_found", "error": "error"}


def transform(data: ExtractedData, cache: pd.DataFrame) -> TransformedData:
    """Combine MovieLens data with cached OMDb details into clean, typed table rows."""
    cache_by_movie = {int(r.movie_id): r for r in cache.itertuples(index=False)}
    imdb_by_movie = dict(zip(data.links["movieId"], data.links["imdb_id"]))

    movie_rows: list[dict[str, Any]] = []
    genre_pairs: list[tuple[int, str]] = []
    director_pairs: list[tuple[int, str]] = []

    for movie in data.movies.itertuples(index=False):
        movie_id = int(movie.movieId)
        cached = cache_by_movie.get(movie_id)
        status = STATUS_FROM_CACHE.get(cached.status, "pending") if cached else "pending"
        omdb = _parse_json(cached.response_json, movie_id) if status == "enriched" else {}

        # Release year: MovieLens title first (the original release); OMDb only as fallback.
        year, source = valid_year(year_from_title(movie.title)), "movielens"
        if year is None:
            year, source = valid_year(parse_omdb_year(omdb.get("Year"))), "omdb"
        if year is None:
            source = None

        imdb_id = imdb_by_movie.get(movie_id)
        movie_rows.append({
            "movie_id": movie_id,
            "imdb_id": imdb_id if isinstance(imdb_id, str) else None,
            "title": movie.title,
            "release_year": year,
            "release_year_source": source,
            "release_decade": decade_of(year),
            "plot": clean_text(omdb.get("Plot")),
            "box_office_usd": parse_box_office(omdb.get("BoxOffice")),
            "released_date": parse_released_date(omdb.get("Released")),
            "runtime_minutes": parse_runtime(omdb.get("Runtime")),
            "enrichment_status": status,
            "enriched_at": cached.fetched_at if status == "enriched" else None,
        })
        genre_pairs += [(movie_id, g) for g in split_genres(movie.genres)]
        director_pairs += [(movie_id, d) for d in split_directors(omdb.get("Director"))]

    # Nullable integer columns keep missing values as <NA> instead of turning ints into floats.
    movies = pd.DataFrame(movie_rows).astype({
        "release_year": "Int64", "release_decade": "Int64",
        "box_office_usd": "Int64", "runtime_minutes": "Int64",
    })
    ratings = pd.DataFrame({
        "user_id": data.ratings["userId"],
        "movie_id": data.ratings["movieId"],
        "rating": data.ratings["rating"].astype("float64"),
        "rated_at": unix_to_utc_text(data.ratings["timestamp"]),
    })
    result = TransformedData(
        movies=movies,
        movie_genres=pd.DataFrame(genre_pairs, columns=["movie_id", "genre_name"]),
        movie_directors=pd.DataFrame(director_pairs, columns=["movie_id", "director_name"]),
        ratings=ratings,
    )
    log_transform_summary(result)
    return result


def log_transform_summary(t: TransformedData) -> None:
    """Log what the Transform stage produced, so each run can be checked from the log."""
    m = t.movies
    by_source = m["release_year_source"].value_counts().to_dict()
    logger.info("Transform: %d movies | release year from MovieLens %d, from OMDb %d, unknown %d",
                len(m), by_source.get("movielens", 0), by_source.get("omdb", 0),
                int(m["release_year"].isna().sum()))
    logger.info("Transform: enrichment %s | with director %d | with box office %d | "
                "with release date %d | with runtime %d",
                m["enrichment_status"].value_counts().to_dict(),
                t.movie_directors["movie_id"].nunique(), int(m["box_office_usd"].notna().sum()),
                int(m["released_date"].notna().sum()), int(m["runtime_minutes"].notna().sum()))
    logger.info("Transform: %d distinct genres, %d movie-genre links | %d distinct directors, "
                "%d movie-director links | %d ratings",
                t.movie_genres["genre_name"].nunique(), len(t.movie_genres),
                t.movie_directors["director_name"].nunique(), len(t.movie_directors),
                len(t.ratings))


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run_pipeline(config: Config) -> TransformedData:
    """Run the pipeline stages in order. (Load is added in Sub-step 4.4.)"""
    logger.info("Pipeline started | data=%s | db=%s | max_api_calls=%d | skip_api=%s",
                config.data_dir, config.db_path, config.max_api_calls, config.skip_api)
    logger.info("OMDb API key configured: %s", "yes" if config.omdb_api_key else "NO")

    extracted = extract_csv_sources(config.data_dir)

    conn = connect_db(config.db_path)
    try:
        enrich_with_omdb(conn, extracted, config)
        transformed = transform(extracted, read_cache(conn))
    finally:
        conn.close()

    logger.info("Extract, Enrich and Transform complete. Load is not implemented yet.")
    return transformed


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
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception:
        logger.exception("Pipeline failed with an unexpected error")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())