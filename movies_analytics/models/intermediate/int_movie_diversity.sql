{{ config(materialized='table') }}

WITH movie_diversity AS (
    SELECT
        m.movie_id,
        COUNT(DISTINCT bg.genre_name) AS genre_diversity,
        COUNT(DISTINCT bl.language_code) AS language_diversity

    FROM {{ ref('dim_movies') }} m
    LEFT JOIN {{ ref('bridge_movies_genres') }} bg 
        ON m.movie_id = bg.movie_id
    LEFT JOIN {{ ref('bridge_movies_languages') }} bl 
        ON m.movie_id = bl.movie_id

    GROUP BY m.movie_id
),

movie_ratings AS (
    SELECT
        movie_id,
        NULLIF(avg_rating, 0) AS avg_rating,
        revenue_clean,
        budget_clean
    FROM {{ ref('fact_movie_metrics') }}
),

combined AS (
    SELECT
        d.*,
        r.avg_rating,
        r.revenue_clean,
        r.budget_clean,

        {{ diversity_score_raw('d.genre_diversity', 'd.language_diversity') }} AS diversity_score_raw

    FROM movie_diversity d
    JOIN movie_ratings r 
        ON d.movie_id = r.movie_id
),

final AS (
    SELECT
        *,

        (diversity_score_raw * COALESCE(avg_rating, 0)) AS diversity_index,

        {{ profitability_flag('revenue_clean', 'budget_clean') }} AS profitability_flag,
        {{ language_type('language_diversity') }} AS language_type,

        CASE 
            WHEN revenue_clean > budget_clean THEN 1 ELSE 0
        END AS is_profitable,

        CASE 
            WHEN language_diversity > 1 THEN 1 ELSE 0
        END AS is_multilingual,

        CASE 
            WHEN diversity_score_raw >= (SELECT AVG(diversity_score_raw) FROM combined)
            THEN 'High'
            ELSE 'Low'
        END AS diversity_level,

        RANK() OVER (
            ORDER BY (diversity_score_raw * COALESCE(avg_rating, 0)) DESC
        ) AS diversity_rank

    FROM combined
)

SELECT *
FROM final