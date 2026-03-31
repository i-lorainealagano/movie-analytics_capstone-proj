{{ config(materialized='table') }}  -- or 'incremental' if large

SELECT
    m.movie_id,
    m.title,

    m.release_date_clean::date AS release_date,
    m.release_year,
    m.release_month,

    m.budget_clean,
    m.revenue_clean,

    m.revenue_category,
    m.financial_status, 

    m.has_ratings,
    m."ratings_summary.avg_rating" AS avg_rating,

    m.budget_missing_flag,
    m.revenue_missing_flag,
    m.suspicious_flag,

    m.genres::jsonb AS genres,
    m.spoken_languages::jsonb AS spoken_languages,
    m.production_countries::jsonb AS production_countries,
    m.production_companies::jsonb AS production_companies,
    
    m.data_completeness
    
FROM {{ source('raw', 'clean_movies') }} AS m