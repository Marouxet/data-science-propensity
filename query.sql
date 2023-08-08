WITH user_level AS(
    SELECT 
        (SELECT value FROM ga.customDimensions WHERE index=19) user_id,
        SUM(IF(hits.eventInfo.eventCategory = 'OP :: Transferencias a terceros' AND hits.eventInfo.eventAction = 'Success', 1, 0)) transferencias,
        COUNT(DISTINCT (SELECT value FROM hits.customDimensions WHERE index=4)) distinct_sections,
        MAX(IF((SELECT value FROM hits.customDimensions WHERE index=4)='inversiones', 1, 0)) seccion_inversiones,
        COUNTIF(hits.eventInfo.eventCategory = 'OP :: Constituir plazo fijo' AND hits.eventInfo.eventAction = 'Success') as pf,
    FROM 
        `project.dataset.ga_sessions_*` ga,
        UNNEST(ga.hits) hits
        WHERE
        (_TABLE_SUFFIX 
        BETWEEN 
        FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  '{start_date}'),INTERVAL 60 DAY)) 
        AND 
        FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  '{start_date}'),INTERVAL 1 DAY))
        )
        GROUP BY 1
),

date_level AS(
    SELECT
        date as fecha,
        (SELECT value FROM ga.customDimensions WHERE index=19) user_id,
        IFNULL((SELECT value FROM ga.customDimensions WHERE index=2), 'NULL') user_type,
        IFNULL((SELECT value FROM ga.customDimensions WHERE index=35), 'no') inversor,
        (SELECT value FROM ga.customDimensions WHERE index=36) portfolio
    FROM 
        `project.dataset.ga_sessions_*` ga,
        UNNEST(ga.hits) hits
        WHERE
        (_TABLE_SUFFIX 
        BETWEEN 
        FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  '{start_date}'),INTERVAL 60 DAY)) 
        AND 
        FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  '{start_date}'),INTERVAL 1 DAY))
        )
        GROUP BY 1,2,3,4,5
        ORDER BY 1, 2, 3, 4,5
),

combined AS (
SELECT 
    DISTINCT 
    t1.user_id,
    IF(
        STARTS_WITH(REPLACE(FIRST_VALUE(t1.portfolio) OVER ( PARTITION BY user_id ORDER BY fecha DESC),'sin tenencia', 'sin_tenencia'),' '),
        SUBSTR(REPLACE(FIRST_VALUE(t1.portfolio) OVER ( PARTITION BY user_id ORDER BY fecha DESC),'sin tenencia', 'sin_tenencia'), 2),
        REPLACE(FIRST_VALUE(t1.portfolio) OVER ( PARTITION BY user_id ORDER BY fecha DESC),'sin tenencia', 'sin_tenencia')
    ) AS portfolio,
    FIRST_VALUE(t1.inversor) OVER ( PARTITION BY user_id ORDER BY fecha DESC) AS inversor,
    FIRST_VALUE(t1.user_type) OVER ( PARTITION BY user_id ORDER BY fecha DESC) AS user_type,
    t2.* EXCEPT (user_id),
FROM 
    date_level t1
LEFT JOIN user_level t2 USING(user_id)
)

SELECT 
    CURRENT_DATE() as query_date,
    user_id,
    IFNULL(
    IF (
        portfolio='sin_tenencia',
        0,
        ARRAY_LENGTH(SPLIT(portfolio, ' '))
        ),
        0) n_porfolios,
    IF(REGEXP_CONTAINS(portfolio, "acciones-ars"),1,0) acciones_ars,
    IF(REGEXP_CONTAINS(portfolio, "plazos-fijos-usd"),1,0) plazos_fijos_usd,
    IF(REGEXP_CONTAINS(portfolio, "plazos-fijos-ars"),1,0) plazos_fijos_ars,
    IF(REGEXP_CONTAINS(portfolio, "bonos-usd"),1,0) bonos_usd,
    IF(REGEXP_CONTAINS(portfolio, "bonos-ars"),1,0) bonos_ars,
    IF(REGEXP_CONTAINS(portfolio, "fima-usd"),1,0) fima_usd,
    IF(REGEXP_CONTAINS(portfolio, "fima-ars"),1,0) fima_ars,
    IF(REGEXP_CONTAINS(portfolio, "cedears-ars"),1,0) cedears_ars,
    IF(REGEXP_CONTAINS(portfolio, "cedears-usd"),1,0) cedears_usd,
    IF(inversor = 'si',1,0) inversor, 
    CASE user_type
        WHEN "MASIVO" THEN 1
        WHEN "EMINENT" THEN 2
        ELSE 3
    END user_type,
    pf,
    transferencias, 
    distinct_sections, 
    seccion_inversiones
FROM 
    combined
WHERE 
    user_id is not null