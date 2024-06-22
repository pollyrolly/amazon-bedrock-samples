SELECT trim(q."database") as dbname,
       trim(s.perm_table_name) AS table_name,
       COALESCE(SUM(ABS(datediff (microsecond,COALESCE(b.starttime,d.starttime,s.starttime),CASE WHEN COALESCE(b.endtime,d.endtime,s.endtime) >COALESCE(b.starttime,d.starttime,s.starttime) THEN COALESCE(b.endtime,d.endtime,s.endtime) ELSE COALESCE(b.starttime,d.starttime,s.starttime) END)))/ 1000000::NUMERIC(38,4),0) AS alert_seconds,
       COALESCE(SUM(COALESCE(b.rows,d.rows,s.rows)),0) AS alert_rowcount,
       trim(split_part (l.event,':',1)) AS alert_event,
       substring(trim(l.solution),1,200) AS alert_solution,
       MAX(l.query) AS alert_sample_query,
       COUNT(DISTINCT l.query) alert_querycount
FROM stl_alert_event_log AS l
  LEFT JOIN stl_scan AS s
         ON s.query = l.query
        AND s.slice = l.slice
        AND s.segment = l.segment
        AND s.userid > 1
        AND s.perm_table_name NOT IN ('Internal Worktable','S3')
		AND s.perm_table_name NOT LIKE ('volt_tt%')
		AND s.perm_table_name NOT LIKE ('mv_tbl__auto_mv%')			
  LEFT JOIN stl_dist AS d
         ON d.query = l.query
        AND d.slice = l.slice
        AND d.segment = l.segment
        AND d.userid > 1
  LEFT JOIN stl_bcast AS b
         ON b.query = l.query
        AND b.slice = l.slice
        AND b.segment = l.segment
        AND b.userid > 1    
  LEFT JOIN stl_query AS q   
         ON q.query = l.query
        AND q.xid = l.xid
        AND q.userid > 1           
WHERE l.userid > 1
  AND trim(s.perm_table_name) IS NOT NULL
  AND l.event_time >= dateadd(day,- 7,CURRENT_DATE)
GROUP BY 1,2,5,6
ORDER BY alert_seconds DESC;
