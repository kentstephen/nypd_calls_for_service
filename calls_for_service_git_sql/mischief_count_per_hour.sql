SELECT
    SUBSTRING(incident_time, 1, 2) AS hour,
    COUNT(*) AS incident_count
FROM sch_nypd_calls_tables.tb_call_data
WHERE typ_desc LIKE '%MISCHIEF%' AND incident_date BETWEEN '2020-01-01' AND '2020-01-01'
GROUP BY hour
ORDER BY hour;

SELECT COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' THEN 1 END)
FROM sch_nypd_calls_tables.tb_call_data
WHERE incident_date BETWEEN '2018-01-01' AND '2018-12-31'