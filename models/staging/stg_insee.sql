-- Staging INSEE : nettoyage des données brutes INSEE
-- Extraction des données pertinentes pour l'analyse

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'INSEE_RAW') }}
),

cleaned AS (
    SELECT
        C1 AS REGION,
        CASE
            WHEN C1 = 'Centre-Val de Loire' THEN 'Centre-Val-de-Loire'
            ELSE C1
        END AS REGION_HARMONISEE,
        C2 AS POP_ENSEMBLE_0_19,
        C3 AS POP_ENSEMBLE_20_39,
        C4 AS POP_ENSEMBLE_40_59,
        C5 AS POP_ENSEMBLE_60_74,
        C6 AS POP_ENSEMBLE_75_PLUS,
        C7 AS POP_TOTAL
    FROM source
    WHERE C1 IS NOT NULL
    AND C1 NOT IN (
        'Régions',
        'France métropolitaine',
        'France',
        'Estimation de population au 1er janvier, par région, sexe et grande classe d''âge'
    )
)

SELECT * FROM cleaned