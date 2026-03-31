{{ config(materialized='table') }}

SELECT
    m.movie_id,
    dg.genre_id
FROM {{ ref('stg_movies') }} AS m
LEFT JOIN LATERAL (
    SELECT json_array_elements_text(m.genres::json) AS genre_name
    WHERE m.genres LIKE '[%'
) AS j ON true
JOIN {{ ref('dim_genres') }} AS dg
    ON COALESCE(j.genre_name, m.genres) = dg.genre_name
WHERE COALESCE(j.genre_name, m.genres) IS NOT NULL