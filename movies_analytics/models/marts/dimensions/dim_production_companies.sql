{{ config(materialized='table') }}

WITH split_companies AS (
    SELECT
        DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(company, '[\[\]\"㉿]', '', 'g'))) AS company_name
    FROM {{ ref('stg_movies') }},
    UNNEST(string_to_array(production_companies, ',')) AS company
)
SELECT *
FROM split_companies
WHERE company_name != ''