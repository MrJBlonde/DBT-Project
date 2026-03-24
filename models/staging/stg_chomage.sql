-- Staging chômage : nettoyage des données brutes INSEE chômage
-- Source : INSEE_CHOMAGE dans Snowflake

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'INSEE_CHOMAGE') }}
),

cleaned AS (
    SELECT
        REGION,
        -- Harmonisation des noms de régions
        CASE
            WHEN REGION = 'Centre-Val de Loire' THEN 'Centre-Val-de-Loire'
            ELSE REGION
        END AS REGION_HARMONISEE,
        CAST(TAUX_CHOMAGE_T3_2025 AS FLOAT) AS TAUX_CHOMAGE_T3_2025,
        CAST(TAUX_CHOMAGE_T3_2024 AS FLOAT) AS TAUX_CHOMAGE_T3_2024
    FROM source
    WHERE REGION IS NOT NULL
    AND REGION != 'France hors Mayotte'
)

SELECT * FROM cleaned