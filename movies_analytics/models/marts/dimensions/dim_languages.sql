-- stg_movies_countries.sql
{{ config(materialized='table') }}

SELECT
    m.movie_id,
    c->>'name' AS country_name
FROM {{ ref('stg_movies') }} m
CROSS JOIN LATERAL m.production_countries AS c
WHERE m.production_countries IS NOT NULL