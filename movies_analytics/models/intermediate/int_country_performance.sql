{{ config(materialized='table') }}

WITH genre_year_agg AS (
    SELECT
        g.genre_name,
        f.release_year,

        AVG(NULLIF(f.avg_rating, 0)) AS avg_rating,

        COUNT(DISTINCT bg.movie_id) AS total_movies

    FROM {{ ref('bridge_movies_genres') }} bg
    JOIN {{ ref('dim_genres') }} g 
        ON bg.genre_id = g.genre_id
    JOIN {{ ref('fact_movie_metrics') }} f 
        ON bg.movie_id = f.movie_id

    WHERE f.release_year IS NOT NULL

    GROUP BY g.genre_name, f.release_year
),

with_lag AS (
    SELECT
        genre_name,
        release_year,
        avg_rating,
        total_movies,

        {{ prev_year_metric('avg_rating', 'release_year', 'genre_name') }} AS prev_year_avg_rating

    FROM genre_year_agg
),

final AS (
    SELECT
        genre_name,
        release_year,
        avg_rating,
        total_movies,
        prev_year_avg_rating,

        {{ rating_change('avg_rating', 'prev_year_avg_rating') }} AS rating_change,

        -- 🔥 ADD RANK
        RANK() OVER (
            PARTITION BY release_year
            ORDER BY avg_rating DESC
        ) AS genre_rank

    FROM with_lag
)

SELECT *
FROM final
ORDER BY release_year, genre_rank