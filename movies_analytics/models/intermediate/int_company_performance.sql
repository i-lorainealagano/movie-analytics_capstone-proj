{{ config(materialized='table') }}

WITH company_year_agg AS (
    SELECT
        c.company_name,
        f.release_year,

        COUNT(DISTINCT bc.movie_id) AS total_movies,

        AVG(NULLIF(f.avg_rating, 0)) AS avg_rating,

        SUM(f.revenue_clean) AS total_revenue,
        SUM(f.budget_clean) AS total_budget

    FROM {{ ref('bridge_movies_companies') }} bc
    JOIN {{ ref('dim_production_companies') }} c 
        ON bc.company_id = c.company_id
    JOIN {{ ref('fact_movie_metrics') }} f 
        ON bc.movie_id = f.movie_id

    WHERE f.release_year IS NOT NULL

    GROUP BY c.company_name, f.release_year
),

with_lag AS (
    SELECT
        *,

        {{ prev_year_metric('avg_rating', 'release_year', 'company_name') }} AS prev_year_avg_rating

    FROM company_year_agg
),

final AS (
    SELECT
        company_name,
        release_year,
        total_movies,
        avg_rating,
        total_revenue,
        total_budget,
        prev_year_avg_rating,

        {{ rating_change('avg_rating', 'prev_year_avg_rating') }} AS rating_change,

        {{ profitability_flag('total_revenue', 'total_budget') }} AS profitability_flag,

        RANK() OVER (
            PARTITION BY release_year
            ORDER BY total_revenue DESC
        ) AS company_revenue_rank,

        RANK() OVER (
            PARTITION BY release_year
            ORDER BY avg_rating DESC
        ) AS company_rating_rank

    FROM with_lag
)

SELECT *
FROM final
ORDER BY release_year, company_revenue_rank