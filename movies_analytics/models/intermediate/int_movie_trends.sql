{{ config(materialized='table') }}

WITH yearly_agg AS (
    SELECT
        m.release_year,
        COUNT(DISTINCT m.movie_id) AS total_movies,
        AVG(CASE WHEN f.total_ratings >= 50 THEN f.avg_rating END) AS avg_rating,
        SUM(f.avg_rating * f.total_ratings) / NULLIF(SUM(f.total_ratings),0) AS weighted_avg_rating,
        COUNT(CASE WHEN f.has_ratings THEN 1 END) AS rated_movies,
        SUM(f.revenue_clean) AS total_revenue,
        SUM(f.budget_clean) AS total_budget,
        SUM(CASE WHEN f.multi_language = TRUE THEN 1 ELSE 0 END) AS multi_language_movies,
        SUM(CASE WHEN f.multi_country = TRUE THEN 1 ELSE 0 END) AS multi_country_movies
    FROM {{ ref('dim_movies') }} m
    JOIN {{ ref('fact_movie_metrics') }} f 
        ON m.movie_id = f.movie_id
    WHERE m.release_year IS NOT NULL
    GROUP BY m.release_year
),

all_years AS (
    SELECT generate_series(1870, 2025) AS release_year
),

filled_years AS (
    SELECT
        y.release_year,
        COALESCE(a.total_movies, 0) AS total_movies,
        a.avg_rating,
        a.weighted_avg_rating,
        COALESCE(a.total_revenue, 0) AS total_revenue,
        COALESCE(a.total_budget, 0) AS total_budget,
        COALESCE(a.multi_language_movies, 0) AS multi_language_movies,
        COALESCE(a.multi_country_movies, 0) AS multi_country_movies
    FROM all_years y
    LEFT JOIN yearly_agg a
        ON y.release_year = a.release_year
),

with_lag AS (
    SELECT
        *,
        {{ prev_year_metric('avg_rating', 'release_year') }} AS prev_year_avg_rating,
        {{ prev_year_metric('total_movies', 'release_year') }} AS prev_year_total_movies,
        {{ prev_year_metric('total_revenue', 'release_year') }} AS prev_year_revenue
    FROM filled_years
),

final AS (
    SELECT
        release_year,
        total_movies,
        avg_rating,
        weighted_avg_rating,
        total_revenue,
        total_budget,
        multi_language_movies,
        multi_country_movies,
        prev_year_avg_rating,
        prev_year_total_movies,
        prev_year_revenue,
        {{ rating_change('avg_rating', 'prev_year_avg_rating') }} AS rating_change,
        {{ rating_change('total_movies', 'prev_year_total_movies') }} AS movie_growth,
        {{ rating_change('total_revenue', 'prev_year_revenue') }} AS revenue_growth,
        CASE WHEN total_budget > 0 THEN total_revenue - total_budget ELSE NULL END AS profit,
        CASE WHEN total_revenue >= total_budget THEN 'Profitable' ELSE 'Not Profitable' END AS profitability_status,
        {{ rating_flag('avg_rating') }} AS rating_flag,
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM with_lag
)

SELECT *
FROM final
ORDER BY release_year