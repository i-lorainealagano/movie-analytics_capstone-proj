{{ config(materialized='table') }}

SELECT
    m.movie_id,
    TRIM(genre) AS genre_name
FROM {{ ref('stg_movies') }} m,
UNNEST(string_to_array(m.genres, ',')) AS genre
WHERE m.genres IS NOT NULL
AND TRIM(genre) != ''