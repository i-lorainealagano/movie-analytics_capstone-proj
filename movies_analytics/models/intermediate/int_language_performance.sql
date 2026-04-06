{{ config(materialized='table') }}

WITH language_year_agg AS (
    SELECT
        bl.language_code,
        f.release_year,
        AVG(NULLIF(f.avg_rating, 0)) AS avg_rating,
        COUNT(DISTINCT bl.movie_id) AS total_movies

    FROM {{ ref('bridge_movies_languages') }} bl
    JOIN {{ ref('fact_movie_metrics') }} f 
        ON bl.movie_id = f.movie_id

    WHERE f.release_year IS NOT NULL
    GROUP BY bl.language_code, f.release_year
),

with_lag AS (
    SELECT
        *,
        {{ prev_year_metric('avg_rating', 'release_year', 'language_code') }} AS prev_year_avg_rating
    FROM language_year_agg
)

SELECT
    language_code,
    release_year,
    avg_rating,
    total_movies,
    prev_year_avg_rating,

    {{ rating_change('avg_rating', 'prev_year_avg_rating') }} AS rating_change,

    RANK() OVER (
        PARTITION BY release_year 
        ORDER BY avg_rating DESC
    ) AS language_rank

FROM with_lag
ORDER BY release_year, language_rank
