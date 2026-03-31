{{ config(materialized='table') }}

WITH yearly_agg AS (
    SELECT
        m.release_year,

        COUNT(DISTINCT m.movie_id) AS total_movies,

        AVG(NULLIF(f.avg_rating, 0)) AS avg_rating,

        SUM(f.revenue_clean) AS total_revenue,
        SUM(f.budget_clean) AS total_budget

    FROM {{ ref('dim_movies') }} AS m
    JOIN {{ ref('fact_movie_metrics') }} AS f 
        ON m.movie_id = f.movie_id

    WHERE m.release_year IS NOT NULL

    GROUP BY m.release_year
),

with_lag AS (
    SELECT
        *,

        {{ prev_year_metric('avg_rating', 'release_year') }} AS prev_year_avg_rating,
        {{ prev_year_metric('total_movies', 'release_year') }} AS prev_year_total_movies

    FROM yearly_agg
),

final AS (
    SELECT
        *,

        {{ rating_change('avg_rating', 'prev_year_avg_rating') }} AS rating_change,
        {{ rating_change('total_movies', 'prev_year_total_movies') }} AS movie_growth,

        CASE 
            WHEN total_revenue >= total_budget THEN 'Profitable'
            ELSE 'Not Profitable'
        END AS profitability_status,

        {{ rating_flag('avg_rating') }} AS rating_flag

    FROM with_lag
)

SELECT *
FROM final
ORDER BY release_year