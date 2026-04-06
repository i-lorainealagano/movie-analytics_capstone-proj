{{ config(materialized='table') }}

SELECT
    m.id AS movie_id,
    m.title,
    m.release_date_clean::date AS release_date,
    m.release_year,
    m.release_month,

    m.budget_clean::numeric AS budget_clean,
    m.revenue_clean::numeric AS revenue_clean,
    m.revenue_category,
    m.financial_status,

    CASE 
        WHEN m."ratings_summary.total_ratings" > 0 THEN m."ratings_summary.avg_rating"
    END AS avg_rating,
    m."ratings_summary.total_ratings"::int AS total_ratings,
    m."ratings_summary.total_ratings" > 0 AS has_ratings,

    m.budget_missing_flag::boolean,
    m.revenue_missing_flag::boolean,
    m.suspicious_flag::boolean,

    CASE 
        WHEN jsonb_array_length(spoken_languages::jsonb) > 1 THEN TRUE
        ELSE FALSE
    END AS multi_language,

    CASE 
        WHEN jsonb_array_length(production_countries::jsonb) > 1 THEN TRUE
        ELSE FALSE
    END AS multi_country,

    jsonb_array_length(spoken_languages::jsonb) AS num_languages,
    jsonb_array_length(production_countries::jsonb) AS num_countries,
    
    m.genres,
    m.production_companies,
    m.production_countries,
    m.spoken_languages,
    m.data_completeness

FROM {{ source('movies_src', 'raw_movies') }} m