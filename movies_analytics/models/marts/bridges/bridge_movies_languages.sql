{{ config(materialized='table') }}

SELECT
    m.movie_id,
    d.language_id
FROM {{ ref('stg_movies') }} m
JOIN {{ ref('dim_languages') }} d
    ON m.language_name = d.language_name