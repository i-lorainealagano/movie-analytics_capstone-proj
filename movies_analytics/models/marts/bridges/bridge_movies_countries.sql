{{ config(materialized='table') }}

SELECT
    m.movie_id,
    d.country_id
FROM {{ ref('stg_movies') }} m
JOIN {{ ref('dim_countries') }} d
    ON m.country_name = d.country_name