-- =====================================================================
-- queries.sql - analytical questions on movies.db (SQLite 3.25+)
-- =====================================================================
-- Run all queries with readable output:   python scripts/run_queries.py
--
-- Conventions used in every query
--   * Averages are rounded only for display; ranking uses the exact value.
--   * Ties are never hidden: RANK() keeps every row that shares a position.
--   * Every ORDER BY ends with a unique tie-breaker, so output is deterministic.
--   * Ratings join movies on movie_id (the primary key), and each
--     (user, movie) pair exists once, so no join can duplicate a rating.
--
-- Each query starts with a line beginning "-- @query"; the runner script
-- uses those lines to split this file. Comments contain no semicolons.
-- =====================================================================


-- @query Q1 | Which movie has the highest average rating? (literal answer, all ties)
-- Interpretation: average of all user ratings per movie, with no minimum
-- number of ratings. In this dataset many movies have a single 5.0 rating,
-- so the literal answer is a large tie. All tied movies are returned, most
-- rated first.
WITH movie_avg AS (
    SELECT m.movie_id,
           m.title,
           AVG(r.rating) AS avg_rating,
           COUNT(*)      AS n_ratings
    FROM ratings AS r
    JOIN movies  AS m ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY avg_rating DESC) AS rnk
    FROM movie_avg
)
SELECT movie_id,
       title,
       ROUND(avg_rating, 3) AS avg_rating,
       n_ratings
FROM ranked
WHERE rnk = 1
ORDER BY n_ratings DESC, title, movie_id;


-- @query Q1b | OPTIONAL - Highest average rating among movies with at least 50 ratings
-- Not part of the assignment's wording. Added because an average of one or
-- two ratings is not a reliable measure. The threshold (50) was fixed before
-- looking at the results. 450 movies qualify in the MovieLens small dataset.
-- Change min_ratings below to use another threshold.
WITH params AS (
    SELECT 50 AS min_ratings
),
movie_avg AS (
    SELECT m.movie_id,
           m.title,
           AVG(r.rating) AS avg_rating,
           COUNT(*)      AS n_ratings
    FROM ratings AS r
    JOIN movies  AS m ON m.movie_id = r.movie_id
    GROUP BY m.movie_id, m.title
    HAVING COUNT(*) >= (SELECT min_ratings FROM params)
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY avg_rating DESC) AS rnk
    FROM movie_avg
)
SELECT rnk AS position,
       movie_id,
       title,
       ROUND(avg_rating, 3) AS avg_rating,
       n_ratings
FROM ranked
WHERE rnk <= 5
ORDER BY rnk, n_ratings DESC, title, movie_id;


-- @query Q2 | What are the top 5 movie genres that have the highest average rating?
-- Interpretation: average of every individual rating given to movies in the
-- genre (each rating counts once per genre of its movie). A movie with
-- several genres contributes to each of them, which is intended.
-- Movies listed as '(no genres listed)' have no genre rows and are excluded.
-- For comparison, avg_of_movie_averages weights every movie equally instead.
-- RANK() keeps a 5th-place tie instead of cutting it off arbitrarily.
WITH genre_stats AS (
    SELECT g.genre_id,
           g.genre_name,
           AVG(r.rating)              AS avg_rating,
           COUNT(*)                   AS n_ratings,
           COUNT(DISTINCT r.movie_id) AS n_movies
    FROM ratings      AS r
    JOIN movie_genres AS mg ON mg.movie_id = r.movie_id
    JOIN genres       AS g  ON g.genre_id  = mg.genre_id
    GROUP BY g.genre_id, g.genre_name
),
movie_avg AS (
    SELECT movie_id, AVG(rating) AS movie_avg
    FROM ratings
    GROUP BY movie_id
),
genre_movie_avg AS (
    SELECT mg.genre_id, AVG(ma.movie_avg) AS avg_of_movie_averages
    FROM movie_genres AS mg
    JOIN movie_avg    AS ma ON ma.movie_id = mg.movie_id
    GROUP BY mg.genre_id
),
ranked AS (
    SELECT gs.*, gma.avg_of_movie_averages,
           RANK() OVER (ORDER BY gs.avg_rating DESC) AS rnk
    FROM genre_stats     AS gs
    JOIN genre_movie_avg AS gma ON gma.genre_id = gs.genre_id
)
SELECT rnk AS position,
       genre_name,
       ROUND(avg_rating, 4)            AS avg_rating,
       n_ratings,
       n_movies,
       ROUND(avg_of_movie_averages, 4) AS avg_of_movie_averages
FROM ranked
WHERE rnk <= 5
ORDER BY rnk, genre_name;


-- @query Q3 | Who is the director with the most movies in this dataset?
-- Interpretation: number of distinct movies per director, using OMDb
-- director data. Co-directed films count once for each director (they are
-- stored as separate rows in movie_directors). All tied directors are shown.
-- IMPORTANT: only enriched movies have director data, so the answer covers
-- the enriched subset. enriched_movies / total_movies shows that coverage.
WITH director_counts AS (
    SELECT d.director_id,
           d.director_name,
           COUNT(*) AS n_movies
    FROM movie_directors AS md
    JOIN directors       AS d ON d.director_id = md.director_id
    GROUP BY d.director_id, d.director_name
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY n_movies DESC) AS rnk
    FROM director_counts
)
SELECT director_name,
       n_movies,
       (SELECT COUNT(*) FROM movies WHERE enrichment_status = 'enriched') AS enriched_movies,
       (SELECT COUNT(*) FROM movies)                                       AS total_movies
FROM ranked
WHERE rnk = 1
ORDER BY director_name, director_id;


-- @query Q4 | What is the average rating of movies released each year?
-- Interpretation: "released" means the movie's release year (not the year
-- a rating was given). avg_rating averages every rating of movies released
-- that year. avg_of_movie_averages weights each movie equally instead.
-- Movies whose release year is unknown are excluded, and movies without
-- ratings do not appear (they have no rating to average).
WITH movie_avg AS (
    SELECT m.movie_id,
           m.release_year,
           AVG(r.rating) AS movie_avg,
           COUNT(*)      AS n_ratings
    FROM movies  AS m
    JOIN ratings AS r ON r.movie_id = m.movie_id
    WHERE m.release_year IS NOT NULL
    GROUP BY m.movie_id, m.release_year
)
SELECT release_year,
       ROUND(SUM(movie_avg * n_ratings) / SUM(n_ratings), 3) AS avg_rating,
       SUM(n_ratings)                                        AS n_ratings,
       COUNT(*)                                              AS n_movies,
       ROUND(AVG(movie_avg), 3)                              AS avg_of_movie_averages
FROM movie_avg
GROUP BY release_year
ORDER BY release_year;