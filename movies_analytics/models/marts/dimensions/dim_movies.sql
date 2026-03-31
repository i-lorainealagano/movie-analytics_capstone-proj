{{ config(materialized='table') }}

SELECT
    movie_id,
    title,
    release_year,
    release_month
FROM {{ ref('stg_movies') }}
