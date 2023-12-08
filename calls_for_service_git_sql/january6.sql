WITH jan6_counts AS (
    SELECT
        typ_desc,
        COUNT(*) AS jan6_count
    FROM sch_nypd_calls_tables.tb_call_data
    WHERE DATE(incident_date) = '2018-01-06'
    GROUP BY typ_desc
),

daily_counts AS (
    SELECT
        typ_desc,
        DATE(incident_date) AS call_date,
        COUNT(*) AS daily_count
    FROM sch_nypd_calls_tables.tb_call_data
    GROUP BY typ_desc, DATE(incident_date)
)

SELECT
    j.typ_desc,
    j.jan6_count,
    AVG(d.daily_count) AS avg_daily_count
FROM jan6_counts j
JOIN daily_counts d ON j.typ_desc = d.typ_desc
GROUP BY j.typ_desc, j.jan6_count
ORDER BY j.jan6_count DESC;


WITH jan_6_counts AS (
    SELECT
        typ_desc,
        COUNT(*) as jan_6_count
    FROM
        sch_nypd_calls_tables.tb_call_data
    WHERE
        DATE(incident_date) = '2018-01-06'
    GROUP BY
        typ_desc
),

daily_counts AS (
    SELECT
        typ_desc,
        DATE(incident_date) AS call_date,
        COUNT(*) AS daily_count
    FROM
        sch_nypd_calls_tables.tb_call_data
    GROUP BY
        typ_desc, DATE(incident_date)
)

SELECT
    j.typ_desc,
    j.jan_6_count,
    AVG(d.daily_count) AS avg_daily_count
FROM
    jan_6_counts j
JOIN
    daily_counts d
ON
    j.typ_desc = d.typ_desc
GROUP BY j.typ_desc, j.jan_6_count
ORDER BY j.jan_6_count DESC;
t