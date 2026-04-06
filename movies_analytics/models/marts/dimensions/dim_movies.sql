{{ config(materialized='table') }}

SELECT
    movie_id,
    title,
    release_date,
    release_year,
    release_month,
    budget_clean,
    revenue_clean,
    revenue_category,
    financial_status,
    avg_rating,
    total_ratings,
    has_ratings,
    data_completeness,
    budget_missing_flag,
    revenue_missing_flag,
    suspicious_flag,

    CASE 
        WHEN suspicious_flag = TRUE THEN 1 ELSE 0
    END AS is_suspicious

FROM {{ ref('stg_movies') }}