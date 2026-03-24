-- Staging étudiants : nettoyage des données brutes OC
-- Gestion des valeurs manquantes et harmonisation des régions

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'ETUDIANTS') }}
),

cleaned AS (
    SELECT
        USER_ID,
        PATH_CATEGORY_NAME,
        AGE_GROUP,
        CASE 
            WHEN GENDER IS NULL OR GENDER = '' THEN 'Non renseigné'
            ELSE GENDER 
        END AS GENDER,
        CASE 
            WHEN REGION = 'Centre-Val de Loire' THEN 'Centre-Val-de-Loire'
            ELSE REGION 
        END AS REGION,
        YEAR_PATH_STARTED
    FROM source
)

SELECT * FROM cleaned