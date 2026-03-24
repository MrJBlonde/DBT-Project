-- Mart démographie : table finale d'analyse sociodémographique
-- Agrégation des données OC + enrichissement INSEE

WITH etudiants AS (
    SELECT * FROM {{ ref('stg_etudiants') }}
),

chomage AS (
    SELECT * FROM {{ ref('stg_chomage') }}
),

aggregated AS (
    SELECT
        e.YEAR_PATH_STARTED,
        e.REGION,
        e.AGE_GROUP,
        e.GENDER,
        COUNT(*) AS NB_ETUDIANTS,
        COUNT(DISTINCT e.USER_ID) AS NB_ETUDIANTS_UNIQUES
    FROM etudiants e
    GROUP BY 1, 2, 3, 4
),

final AS (
    SELECT
        a.*,
        c.TAUX_CHOMAGE_T3_2025,
        c.TAUX_CHOMAGE_T3_2024
    FROM aggregated a
    LEFT JOIN chomage c
        ON REPLACE(a.REGION, '-', ' ') = REPLACE(c.REGION_HARMONISEE, '-', ' ')
)

SELECT * FROM final
ORDER BY YEAR_PATH_STARTED, REGION, AGE_GROUP, GENDER