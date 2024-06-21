SELECT t."database" AS dbname,
       t."schema" AS namespace,
       t."table" table_name,
       t.encoded,
       t.diststyle,
       t.sortkey1,
       t.max_varchar,
       trim(t.sortkey1_enc) AS sortkey1_enc,
       t.sortkey_num,
       t.unsorted,
       t.stats_off,
       t.tbl_rows,
       t.skew_rows,
       t.estimated_visible_rows,
       CASE
         WHEN t.tbl_rows - t.estimated_visible_rows < 0 THEN 0
         ELSE (t.tbl_rows - t.estimated_visible_rows)
       END AS num_rows_marked_for_deletion,
       CASE
         WHEN t.tbl_rows - t.estimated_visible_rows < 0 THEN 0
         ELSE (t.tbl_rows - t.estimated_visible_rows) /
           CASE
             WHEN nvl (t.tbl_rows,0) = 0 THEN 1
             ELSE t.tbl_rows
           END ::NUMERIC(38,4)
       END AS pct_rows_marked_for_deletion,
       t.vacuum_sort_benefit,
       v.vacuum_run_type,
       v.is_last_vacuum_recluster,
       v.last_vacuumed_date,
       v.days_since_last_vacuumed,
       NVL(s.num_qs,0) query_count,
       nvl(sat.table_recommendation_count,0) AS table_recommendation_count,
       c.encoded_column_count,
       c.column_count,
       c.encoded_column_pct::NUMERIC(38,4) AS encoded_column_pct,
       c.encoded_sortkey_count,
       c.distkey_column_count,
       nvl(tc.large_column_size_count,0) AS large_column_size_count,
       tak.alert_sample_query AS sort_key_alert_sample_query,
       nvl(tak.alert_query_count,0) AS sort_key_alert_query_count,
       tas.alert_sample_query AS stats_alert_sample_query,
       nvl(tas.alert_query_count,0) AS stats_alert_query_count,
       tanl.alert_sample_query AS nl_alert_sample_query,
       nvl(tanl.alert_query_count,0) AS nl_alert_query_count,
       tad.alert_sample_query AS distributed_alert_sample_query,
       nvl(tad.alert_query_count,0) AS distributed_alert_query_count,
       tab.alert_sample_query AS distributed_alert_sample_query,
       nvl(tab.alert_query_count,0) AS broadcasted_alert_query_count,
       tax.alert_sample_query AS deleted_alert_sample_query,
       nvl(tax.alert_query_count,0) AS deleted_alert_query_count
FROM SVV_TABLE_INFO t
  INNER JOIN (SELECT attrelid,
                     COUNT(1) column_count,
                     SUM(CASE WHEN attisdistkey = FALSE THEN 0 ELSE 1 END) AS distkey_column_count,
                     SUM(CASE WHEN attencodingtype IN (0,128) THEN 0 ELSE 1 END) AS encoded_column_count,
                     1.0 *SUM(CASE WHEN attencodingtype IN (0,128) THEN 0 ELSE 1 END) / COUNT(1)*100 encoded_column_pct,
                     SUM(CASE WHEN attencodingtype NOT IN (0,128) AND attsortkeyord > 0 THEN 1 ELSE 0 END) AS encoded_sortkey_count
              FROM pg_attribute
              WHERE attnum > 0
              GROUP BY attrelid) c ON (c.attrelid = t.table_id)
  LEFT OUTER JOIN (SELECT tbl,
                          perm_table_name,
                          COUNT(DISTINCT query) num_qs
                   FROM stl_scan s
                   WHERE s.userid > 1
                   AND   s.perm_table_name NOT IN ('Internal Worktable','S3')
                   GROUP BY 1,
                            2) s ON (s.tbl = t.table_id)
  LEFT OUTER JOIN (SELECT DATABASE,
                          table_id,
                          COUNT(1) AS table_recommendation_count
                   FROM svv_alter_table_recommendations
                   GROUP BY DATABASE,
                            table_id) sat
               ON (sat.table_id = t.table_id
              AND sat.database = t.database)
  LEFT OUTER JOIN (SELECT sc.table_catalog AS database_name,
                          sc.table_schema,
                          sc.table_name,
                          SUM(CASE WHEN sc.character_maximum_length > 1000 THEN 1 ELSE 0 END) AS large_column_size_count
                   FROM svv_columns sc
                     INNER JOIN svv_table_info st
                             ON (sc.table_catalog = st.database
                            AND sc.table_schema = st.schema
                            AND sc.table_name = st.table)
                   WHERE sc.data_type IN ('character varying','character')
                   AND   sc.character_maximum_length > 1000
                   AND   sc.table_schema NOT IN ('pg_internal','pg_catalog','pg_automv')
                   GROUP BY sc.table_catalog,
                            sc.table_schema,
                            sc.table_name) tc
               ON (t.database = tc.database_name
              AND tc.table_name = t.table
              AND tc.table_schema = t.schema)
  LEFT OUTER JOIN (SELECT t.database AS database_name,
                          t.schema AS schema_name,
                          t.table AS table_name,
                          v.table_id,
                          CASE
                            WHEN v.status LIKE '%VacuumBG%' THEN 'Automatic'
                            ELSE 'Manual'
                          END AS vacuum_run_type,
                          CASE
                            WHEN v.is_recluster = 0 THEN 'N'
                            WHEN v.is_recluster = 1 THEN 'Y'
                            ELSE NULL
                          END AS is_last_vacuum_recluster,
                          CAST(MAX(v.eventtime) AS DATE) AS last_vacuumed_date,
                          datediff(d,CAST(MAX(v.eventtime) AS DATE),CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)) AS days_since_last_vacuumed
                   FROM stl_vacuum v
                     INNER JOIN svv_table_info t ON (t.table_id = v.table_id)
                   WHERE v.status NOT LIKE '%Started%'
                   GROUP BY 1,
                            2,
                            3,
                            4,
                            5,
                            6) v
               ON (v.database_name = t.database
              AND v.table_name = t.table
              AND v.schema_name = t.schema)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Very selective query filter'
                   GROUP BY 1,
                            2) tak
               ON (tak.database_name = t.database
              AND tak.table_name = t.table)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Missing query planner statistics'
                   GROUP BY 1,
                            2) tas
               ON (tas.database_name = t.database
              AND tas.table_name = t.table)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Nested Loop Join in the query plan'
                   GROUP BY 1,
                            2) tanl
               ON (tanl.database_name = t.database
              AND tanl.table_name = t.table)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Distributed a large number of rows across the network'
                   GROUP BY 1,
                            2) tad
               ON (tad.database_name = t.database
              AND tad.table_name = t.table)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Broadcasted a large number of rows across the network'
                   GROUP BY 1,
                            2) tab
               ON (tab.database_name = t.database
              AND tab.table_name = t.table)
  LEFT OUTER JOIN (SELECT q.database AS database_name,
                          TRIM(s.perm_table_name) AS table_name,
                          MAX(l.query) AS alert_sample_query,
                          COUNT(DISTINCT l.query) AS alert_query_count
                   FROM stl_alert_event_log AS l
                     LEFT JOIN stl_scan AS s
                            ON s.query = l.query
                           AND s.slice = l.slice
                           AND s.segment = l.segment
                           AND s.userid > 1
                           AND s.perm_table_name NOT IN ('Internal Worktable', 'S3') 
                     LEFT JOIN stl_query AS q
                            ON q.query = l.query
                           AND q.xid = l.xid
                           AND q.userid > 1
                   WHERE l.userid > 1
                   AND   TRIM(s.perm_table_name) IS NOT NULL
                   AND   l.event_time >= dateadd(DAY,- 7,CURRENT_DATE)
                   AND   TRIM(split_part(l.event,':',1)) = 'Scanned a large number of deleted rows'
                   GROUP BY 1,
                            2) tax
               ON (tax.database_name = t.database
              AND tax.table_name = t.table)
WHERE t."schema" NOT IN ('pg_internal','pg_catalog','pg_automv')
AND   t."schema" NOT LIKE 'pg_temp%'
ORDER BY tbl_rows DESC;
