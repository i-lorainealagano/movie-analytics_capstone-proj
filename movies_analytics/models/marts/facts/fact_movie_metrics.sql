{{ config(materialized='table') }}

SELECT
    movie_id,
    release_year,

    budget_clean,
    revenue_clean,

    revenue_category,
    financial_status,

    avg_rating,
    has_ratings,

    budget_missing_flag,
    revenue_missing_flag,
    suspicious_flag,
    
    data_completeness
    
FROM {{ ref('stg_movies') }}