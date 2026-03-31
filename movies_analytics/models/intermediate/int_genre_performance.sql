{{ config(materialized='table') }}

WITH genre_year_agg AS (
    SELECT
        g.genre_name,
        f.release_year,
        AVG(f.avg_rating) AS avg_rating,
        COUNT(*) AS total_movies
    FROM {{ ref('bridge_movies_genres') }} bg
    JOIN {{ ref('dim_genres') }} g ON bg.genre_id = g.genre_id
    JOIN {{ ref('fact_movie_metrics') }} f ON bg.movie_id = f.movie_id
    GROUP BY g.genre_name, f.release_year
)

SELECT
    genre_name,
    release_year,
    avg_rating,
    total_movies,
    {{ prev_year_metric('avg_rating', 'release_year', 'genre_name') }} AS prev_year_avg_rating,
    {{ rating_change('avg_rating', prev_year_metric('avg_rating', 'release_year', 'genre_name')) }} AS rating_change
FROM genre_year_agg
ORDER BY genre_name, release_year