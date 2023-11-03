WITH call_counts AS (
  SELECT
    typ_desc AS types_of_calls,
    incident_date::date AS call_date,
    COUNT(*) AS total_count
  FROM
    sch_nypd_calls_tables.tb_call_data
  WHERE
    typ_desc IS NOT NULL
  GROUP BY
    typ_desc,
    call_date
)
SELECT
  types_of_calls,
  SUM(total_count) AS total_count,
  ROUND(AVG(total_count), 2) as avg_daily_count,
  ROUND(STDDEV_POP(total_count), 2) AS stdev_
FROM
  call_counts
GROUP BY
  types_of_calls
ORDER BY
  stdev_ DESC;
