{{ config(materialized='table') }}

SELECT
    m.movie_id,
    c->>'iso_3166_1' AS country_code
FROM {{ ref('stg_movies') }} m,
LATERAL jsonb_array_elements(m.production_countries::jsonb) c
WHERE m.production_countries IS NOT NULL