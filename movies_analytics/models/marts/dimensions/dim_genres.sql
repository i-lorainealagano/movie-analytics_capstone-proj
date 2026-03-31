{{ config(materialized='table') }}

WITH expanded_genres AS (
    SELECT DISTINCT
        g.genre_name
    FROM {{ ref('stg_movies') }} AS m
    
    LEFT JOIN LATERAL (
        SELECT json_array_elements_text(m.genres::json) AS genre_name
    ) AS g ON m.genres LIKE '[%' 
    UNION
    SELECT DISTINCT
        m.genres AS genre_name
    FROM {{ ref('stg_movies') }} AS m
    WHERE m.genres NOT LIKE '[%' 
)
SELECT
    ROW_NUMBER() OVER (ORDER BY genre_name) AS genre_id,
    genre_name
FROM expanded_genres
WHERE genre_name IS NOT NULL