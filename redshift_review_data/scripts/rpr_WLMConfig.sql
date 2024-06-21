SELECT c.wlm_mode,
       scc.service_class::text AS service_class_id,
       CASE
         WHEN scc.service_class BETWEEN 1 AND 4 THEN 'System'
         WHEN scc.service_class = 5 THEN 'Superuser'
         WHEN scc.service_class BETWEEN 6 AND 13 THEN 'Manual WLM'
         WHEN scc.service_class = 14 THEN 'SQA'
         WHEN scc.service_class = 15 THEN 'Redshift Maintenance'
         WHEN scc.service_class BETWEEN 100 AND 107 THEN 'Auto WLM'
       END AS service_class_category,
       trim(scc.name) AS queue_name,
       CASE
         WHEN scc.num_query_tasks = -1 THEN 'auto'
         ELSE scc.num_query_tasks::text
       END AS slots,
       CASE
         WHEN scc.query_working_mem = -1 THEN 'auto'
         ELSE scc.query_working_mem::text
       END AS query_working_memory_mb_per_slot,
       nvl(cast(ROUND(((scc.num_query_tasks*scc.query_working_mem)::NUMERIC/ mem.total_memory_mb::NUMERIC)*100,0)::NUMERIC(38,4) as varchar(12)),'auto') cluster_memory_pct,
       scc.max_execution_time AS query_timeout,
       trim(scc.concurrency_scaling) AS concurrency_scaling,
       trim(scc.query_priority) AS queue_priority,
       nvl(qc.qmr_rule_count,0) AS qmr_rule_count,
       CASE
         WHEN qmr.qmr_rule IS NOT NULL THEN 'Y'
         ELSE 'N'
       END AS is_queue_evictable,
       LISTAGG(DISTINCT TRIM(qmr.qmr_rule),',') within group(ORDER BY rule_name) qmr_rule,
       LISTAGG(TRIM(cnd.condition),', ') condition
FROM stv_wlm_service_class_config scc
  INNER JOIN stv_wlm_classification_config cnd ON scc.service_class = cnd.action_service_class
  CROSS JOIN (SELECT CASE
                       WHEN COUNT(1) > 0 THEN 'auto'
                       ELSE 'manual'
                     END AS wlm_mode
              FROM stv_wlm_service_class_config
              WHERE service_class >= 100) c
  CROSS JOIN (SELECT SUM(num_query_tasks*query_working_mem) AS total_memory_mb
              FROM stv_wlm_service_class_config
              WHERE service_class BETWEEN 6 AND 13) mem
  LEFT OUTER JOIN (SELECT service_class,
                          COUNT(DISTINCT rule_name) AS qmr_rule_count
                   FROM stv_wlm_qmr_config
                   GROUP BY service_class) qc ON (scc.service_class = qc.service_class)
  LEFT OUTER JOIN (SELECT service_class,
                          rule_name,
                          rule_name || ':' || '[' || action || '] ' || metric_name || metric_operator || CAST(metric_value AS VARCHAR(256)) qmr_rule
                   FROM stv_wlm_qmr_config) qmr ON scc.service_class = qmr.service_class
WHERE scc.service_class > 4
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8,
         9,
         10,
         11,
         12
ORDER BY 2 ASC;