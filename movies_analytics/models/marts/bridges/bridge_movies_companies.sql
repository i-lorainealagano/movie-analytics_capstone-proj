{{ config(materialized='table') }}

SELECT
    m.movie_id,
    dc.company_id
FROM {{ ref('stg_movies') }} m
JOIN {{ ref('dim_production_companies') }} dc
    ON m.company_name = dc.company_name