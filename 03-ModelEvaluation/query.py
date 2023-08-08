

QUERY = """
WITH true_value AS (
SELECT 
    (SELECT value FROM ga.customDimensions WHERE index=19) user_id,
    MAX(IF(hits.eventInfo.eventCategory = 'OP :: Constituir plazo fijo' AND hits.eventInfo.eventAction = 'Success', 1, 0)) label
FROM 
    `project-data.dataset-data.ga_sessions_*` ga, UNNEST(ga.hits) hits
WHERE 
    _TABLE_SUFFIX 
    BETWEEN 
    FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  "{start_date}"),INTERVAL 7 DAY)) 
    AND 
    FORMAT_DATE("%Y%m%d",DATE_SUB(PARSE_DATE('%Y-%m-%d',  "{start_date}"),INTERVAL 1 DAY)) 
GROUP BY 1 
)

SELECT 
    t2.user_id,
    t1.proba as predicted_value,
    t2.label as true_value,
    t1.model,
    t1.predicted_week
FROM `project.dataset.ma_predictions_table` t1
RIGHT JOIN true_value t2 USING(user_id)
WHERE t1.predicted_week = CAST(DATE_SUB(PARSE_DATE("%Y-%m-%d","{start_date}"),INTERVAL 7 DAY) AS STRING)
"""