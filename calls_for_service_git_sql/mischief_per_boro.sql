-- We're selecting the total counts where the call description includes mischief and then separating by boro per day
SELECT
    DATE(incident_date) AS date,
 --   COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' THEN 1 ELSE NULL END) AS total_mischief_count,
    COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' AND boro_nm = 'BROOKLYN' THEN 1 END) AS brooklyn_count,
    COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' AND boro_nm = 'MANHATTAN'THEN 1 END) AS manhattan_count,
    COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' AND boro_nm = 'QUEENS' THEN 1 END) AS queens_count,
    COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' AND boro_nm = 'STATEN ISLAND' THEN 1 END) AS staten_island_count,
    COUNT(CASE WHEN typ_desc LIKE '%MISCHIEF%' AND boro_nm = 'BRONX' THEN 1 END) AS bronx_count
FROM
    sch_nypd_calls_tables.tb_call_data AS tcd
GROUP BY
    incident_date
ORDER BY
    incident_date;
