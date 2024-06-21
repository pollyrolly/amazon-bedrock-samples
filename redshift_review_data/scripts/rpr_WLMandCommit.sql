SELECT IQ.*,
       (IQ.wlm_queue_time_ms/ IQ.wlm_start_commit_time_ms)*100.0::numeric(6,2) AS pct_wlm_queue_time,
       (IQ.exec_only_time_ms/ IQ.wlm_start_commit_time_ms)*100.0::numeric(6,2) AS pct_exec_only_time,
       (IQ.commit_queue_time_ms/ IQ.wlm_start_commit_time_ms)*100.0::numeric(6,2) pct_commit_queue_time,
       (IQ.commit_time_ms/ IQ.wlm_start_commit_time_ms)*100.0::numeric(6,2) pct_commit_time
FROM (SELECT TRUNC(b.starttime) AS DAY,
             d.service_class,
             rtrim(s.name) as queue_name,
             c.node,
             COUNT(DISTINCT c.xid) AS count_commit_xid,
             SUM(datediff ('microsec',d.service_class_start_time,c.endtime)*0.001)::numeric(38,4) AS wlm_start_commit_time_ms,
             SUM(datediff ('microsec',d.queue_start_time,d.queue_end_time)*0.001)::numeric(38,4) AS wlm_queue_time_ms,
             SUM(datediff ('microsec',b.starttime,b.endtime)*0.001)::numeric(38,4) AS exec_only_time_ms,
             SUM(datediff ('microsec',c.startwork,c.endtime)*0.001)::numeric(38,4) commit_time_ms,
             SUM(datediff ('microsec',DECODE(c.startqueue,'2000-01-01 00:00:00',c.startwork,c.startqueue),c.startwork)*0.001)::numeric(38,4) commit_queue_time_ms
      FROM stl_query b,
           stl_commit_stats c,
           stl_wlm_query d,
           stv_wlm_service_class_config s
      WHERE b.xid = c.xid
      AND   b.query = d.query
      AND   c.xid > 0
      AND d.service_class = s.service_class
      GROUP BY 1,2,3,4
      ORDER BY 1,2,3,4) IQ;