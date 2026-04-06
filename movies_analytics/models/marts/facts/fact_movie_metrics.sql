{{ config(materialized='table') }}

WITH primary_country_cte AS (
    SELECT
        movie_id,
        MIN(country_code) AS primary_country
    FROM {{ ref('bridge_movies_countries') }}
    GROUP BY movie_id
),

primary_country_with_name AS (
    SELECT
        pc.movie_id,
        pc.primary_country,
        c.country_name AS primary_country_name
    FROM primary_country_cte pc
    LEFT JOIN {{ ref('dim_countries') }} c
        ON pc.primary_country = c.country_code
)

SELECT
    sm.movie_id,
    sm.title,
    sm.release_year,
    sm.budget_clean,
    sm.revenue_clean,
    sm.revenue_category,
    sm.financial_status,
    sm.avg_rating,
    sm.total_ratings,
    sm.has_ratings,
    sm.budget_missing_flag,
    sm.revenue_missing_flag,
    sm.suspicious_flag,
    sm.multi_language,
    sm.multi_country,
    sm.num_languages,
    sm.num_countries,
    sm.data_completeness,
    
    CASE 
        WHEN sm.budget_clean > 0 THEN sm.revenue_clean - sm.budget_clean
        ELSE NULL
    END AS profit,

    CASE 
        WHEN sm.budget_clean > 0 THEN sm.revenue_clean / sm.budget_clean
        ELSE NULL
    END AS roi,

    CASE 
        WHEN sm.multi_country = TRUE THEN 1 ELSE 0
    END AS is_multi_country,

    CASE 
        WHEN sm.multi_language = TRUE THEN 1 ELSE 0
    END AS is_multilingual,

    CASE 
        WHEN sm.multi_language = TRUE THEN 'Multi-language'
        ELSE 'Single-language'
    END AS language_type,

    pc.primary_country, 
    pc.primary_country_name,

    CASE 
        WHEN sm.multi_country = TRUE THEN 'International'
        ELSE 'Single-country'
    END AS production_type

FROM {{ ref('stg_movies') }} sm
LEFT JOIN primary_country_with_name pc
    ON sm.movie_id = pc.movie_id
WHERE sm.release_year IS NOT NULL