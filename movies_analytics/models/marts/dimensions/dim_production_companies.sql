-- stg_movies_companies.sql
{{ config(materialized='table') }}

SELECT
    m.movie_id,
    company->>'name' AS company_name
FROM {{ ref('stg_movies') }} m
CROSS JOIN LATERAL m.production_companies AS company  
WHERE m.production_companies IS NOT NULL