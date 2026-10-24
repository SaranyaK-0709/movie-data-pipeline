-- =====================================================================
-- schema.sql - Movie Data Pipeline (SQLite)
-- =====================================================================
-- Design summary
--   movies          one row per MovieLens movie, plus OMDb enrichment
--   genres          lookup table of distinct genre names
--   movie_genres    many-to-many: movies <-> genres
--   directors       lookup table of distinct director names
--   movie_directors many-to-many: movies <-> directors (co-directors)
--   ratings         one row per (user, movie) rating
--   omdb_cache      raw OMDb API responses (makes enrichment resumable)
--
-- Notes
--   * Every statement uses IF NOT EXISTS, so this script can be applied
--     on every pipeline run without error or data loss.
--   * STRICT tables (SQLite >= 3.37) reject values of the wrong type.
--     Plain SQLite would silently store e.g. the text 'abc' in an
--     INTEGER column.
--   * SQLite does NOT enforce foreign keys unless each connection runs
--     PRAGMA foreign_keys = ON; etl.py does this on every connection.
--   * Dates/times are stored as ISO-8601 text ('YYYY-MM-DD',
--     'YYYY-MM-DD HH:MM:SS', UTC). SQLite has no native DATE type; ISO
--     text sorts correctly and works with SQLite's date functions.
-- =====================================================================


-- ---------------------------------------------------------------------
-- movies: MovieLens movie data + OMDb enrichment (1 row per movie)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movies (
    movie_id            INTEGER PRIMARY KEY,          -- MovieLens movieId
    imdb_id             TEXT    UNIQUE,               -- e.g. 'tt0114709' (links.csv); NULL if unknown
    title               TEXT    NOT NULL,             -- original MovieLens title, whitespace-trimmed
    release_year        INTEGER,                      -- NULL when unknown
    release_year_source TEXT,                         -- where release_year came from
    release_decade      INTEGER,                      -- feature engineering, e.g. 1990
    plot                TEXT,                         -- OMDb 'Plot'
    box_office_usd      INTEGER,                      -- OMDb 'BoxOffice' "$292,587,330" -> 292587330
    released_date       TEXT,                         -- OMDb 'Released' "16 Jul 2010" -> '2010-07-16'
    runtime_minutes     INTEGER,                      -- OMDb 'Runtime' "148 min" -> 148
    enrichment_status   TEXT    NOT NULL DEFAULT 'pending',
    enriched_at         TEXT,                         -- when OMDb data was fetched (UTC)

    -- NULL is allowed so a movie without an IMDb link is still loaded; SQLite's
    -- UNIQUE permits many NULLs but no two equal IMDb IDs.
    CHECK (imdb_id IS NULL OR imdb_id GLOB 'tt[0-9][0-9][0-9][0-9][0-9][0-9][0-9]*'),
    CHECK (length(trim(title)) > 0),
    CHECK (release_year IS NULL OR release_year BETWEEN 1870 AND 2100),
    CHECK (release_year_source IS NULL OR release_year_source IN ('movielens', 'omdb')),
    -- the source is recorded if and only if a year is present
    CHECK ((release_year IS NULL) = (release_year_source IS NULL)),
    -- the decade must always agree with the year (e.g. 1995 -> 1990)
    CHECK ((release_year IS NULL AND release_decade IS NULL)
           OR release_decade = (release_year / 10) * 10),
    CHECK (box_office_usd IS NULL OR box_office_usd >= 0),
    CHECK (runtime_minutes IS NULL OR runtime_minutes > 0),
    CHECK (enrichment_status IN ('pending', 'enriched', 'not_found', 'error'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_movies_release_year ON movies (release_year);


-- ---------------------------------------------------------------------
-- genres + movie_genres: "Adventure|Animation|Comedy" -> one row per pair
-- '(no genres listed)' is treated as missing data, not as a genre.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS genres (
    genre_id   INTEGER PRIMARY KEY,
    genre_name TEXT    NOT NULL UNIQUE,
    CHECK (length(trim(genre_name)) > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INTEGER NOT NULL REFERENCES movies (movie_id),
    genre_id INTEGER NOT NULL REFERENCES genres (genre_id),
    PRIMARY KEY (movie_id, genre_id)                  -- a pair can exist only once
) STRICT;

-- The primary key already serves lookups by movie_id; this index serves
-- lookups by genre (used by the "top genres" query).
CREATE INDEX IF NOT EXISTS idx_movie_genres_genre_id ON movie_genres (genre_id);


-- ---------------------------------------------------------------------
-- directors + movie_directors: OMDb "Lana Wachowski, Lilly Wachowski"
-- becomes two rows, so co-directed films count for each director.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directors (
    director_id   INTEGER PRIMARY KEY,
    director_name TEXT    NOT NULL UNIQUE,
    CHECK (length(trim(director_name)) > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id    INTEGER NOT NULL REFERENCES movies (movie_id),
    director_id INTEGER NOT NULL REFERENCES directors (director_id),
    PRIMARY KEY (movie_id, director_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_movie_directors_director_id ON movie_directors (director_id);


-- ---------------------------------------------------------------------
-- ratings: one rating per user per movie (verified: 0 duplicate pairs)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ratings (
    user_id  INTEGER NOT NULL,                        -- MovieLens userId
    movie_id INTEGER NOT NULL REFERENCES movies (movie_id),
    rating   REAL    NOT NULL,
    rated_at TEXT    NOT NULL,                        -- Unix timestamp -> 'YYYY-MM-DD HH:MM:SS' UTC
    PRIMARY KEY (user_id, movie_id),                  -- natural key: makes duplicates impossible

    CHECK (user_id > 0),
    -- 0.5 to 5.0 in half-star steps only
    CHECK (rating BETWEEN 0.5 AND 5.0 AND rating * 2 = CAST(rating * 2 AS INTEGER))
) STRICT;

-- Every analytical query joins ratings to movies by movie_id.
CREATE INDEX IF NOT EXISTS idx_ratings_movie_id ON ratings (movie_id);


-- ---------------------------------------------------------------------
-- omdb_cache: raw API results, one row per MovieLens movie.
-- This is an EXTRACT-stage table: it is filled before movies are loaded,
-- so it intentionally has no foreign key to movies. It is keyed by movie_id
-- (always present) rather than imdb_id (missing for some movies in principle).
-- Re-runs read from here instead of calling the API again.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS omdb_cache (
    movie_id      INTEGER PRIMARY KEY,                -- MovieLens movieId
    imdb_id       TEXT,                               -- the IMDb ID that was looked up, if any
    status        TEXT    NOT NULL,                   -- outcome of the last attempt
    lookup_method TEXT    NOT NULL,                   -- how the match was made
    http_status   INTEGER,
    response_json TEXT,                               -- raw body, kept for auditing/re-transforming
    error_message TEXT,
    attempts      INTEGER NOT NULL DEFAULT 1,
    fetched_at    TEXT    NOT NULL,                   -- UTC

    CHECK (status IN ('found', 'not_found', 'error')),
    CHECK (lookup_method IN ('imdb_id', 'title_year', 'none')),
    CHECK (attempts >= 1)
) STRICT;