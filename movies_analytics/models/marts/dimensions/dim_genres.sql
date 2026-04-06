{{ config(materialized='table') }}

WITH split_genres AS (
    SELECT
        TRIM(genre) AS genre_name
    FROM {{ ref('stg_movies') }},
    UNNEST(string_to_array(genres, ',')) AS genre
)

SELECT DISTINCT
    genre_name
FROM split_genres
WHERE genre_name != ''