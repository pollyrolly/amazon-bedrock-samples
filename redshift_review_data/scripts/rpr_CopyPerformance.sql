SELECT a.endtime::DATE AS copy_date,
       trim(d.region) AS aws_region,
       trim(d.s3_bucket) AS s3_bucket,
       trim(d.file_format) AS file_format,
       trim(q."database") as dbname,
       a.tbl AS table_id,
       trim(c.nspname) AS namespace,
       trim(b.relname) AS table_name,
       SUM(a.rows_inserted) AS rows_inserted,
       SUM(d.distinct_files) AS files_scanned,
       SUM(d.mb_scanned) AS mb_scanned,
       (SUM(d.distinct_files)::NUMERIC(19,3) / COUNT(DISTINCT a.query)::NUMERIC(19,3))::NUMERIC(19,3) AS avg_files_per_copy,
       (SUM(d.mb_scanned) / SUM(d.distinct_files)::NUMERIC(19,3))::NUMERIC(19,3) AS avg_file_size_mb,
       MAX(d.files_compressed) AS files_compressed,
       MAX(cluster_slice_count) AS cluster_slice_count,
       AVG(d.used_slice_count) AS avg_used_slice_count,
       COUNT(DISTINCT a.query) no_of_copy,
       MAX(a.query) AS sample_query,
       ROUND((SUM(d.mb_scanned)*1024 *1000000.0 / SUM(d.load_micro)),4) AS scan_rate_kbps,
       ROUND((SUM(a.rows_inserted)*1000000.0 / SUM(a.insert_micro)),4) AS insert_rate_rows_per_second,
       ROUND(SUM(d.copy_duration_micro)/1000000.0,4) AS total_copy_time_secs,
       ROUND(AVG(d.copy_duration_micro)/1000000.0,4) AS avg_copy_time_secs,
       ROUND(SUM(d.compression_micro)/1000000.0,4) AS total_compression_time_secs,
       ROUND(AVG(d.compression_micro)/1000000.0,4) AS avg_compression_time_secs,       
       SUM(d.total_transfer_retries) AS total_transfer_retries,
       SUM(d.distinct_error_files) AS distinct_error_files,
       SUM(d.load_error_count) AS load_error_count
FROM (SELECT query,
             tbl,
             SUM(ROWS) AS rows_inserted,
             MAX(endtime) AS endtime,
             datediff('microsecond',MIN(starttime),MAX(endtime)) AS insert_micro
      FROM stl_insert
      GROUP BY query,
               tbl) a,
     pg_class b,
     pg_namespace c,
     (SELECT b.region,
             l.file_format,
             b.query,
             b.bucket as s3_bucket,
             COUNT(DISTINCT b.bucket||b.key) AS distinct_files,
             COUNT(DISTINCT b.slice) as used_slice_count,
             SUM(b.transfer_size) / 1024 / 1024 AS mb_scanned,
             SUM(b.transfer_time) AS load_micro,
             SUM(b.compression_time) AS compression_micro,
             datediff('microsecond',MIN('2000-01-01'::timestamp + (start_time/1000000.0)* interval '1 second'),MAX('2000-01-01'::timestamp + (end_time/1000000.0)* interval '1 second')) AS copy_duration_micro,
             SUM(b.retries) as total_transfer_retries,
             SUM(nvl(se.distinct_error_files,0)) AS distinct_error_files,
             SUM(nvl(se.load_error_count,0)) AS load_error_count,
             CASE WHEN SUM(b.transfer_size) = SUM(b.data_size) then 'N' else  'Y' end AS files_compressed
      FROM stl_s3client b
      INNER JOIN (select userid, query, MAX(file_format) as file_format FROM stl_load_commits GROUP BY userid, query) l ON (l.userid = b.userid and l.query = b.query)
      LEFT OUTER JOIN (select userid, query, COUNT(DISTINCT bucket||key) AS distinct_error_files, COUNT(1) AS load_error_count FROM stl_s3client_error GROUP BY userid, query) se ON (se.userid = b.userid and se.query = b.query)
      WHERE b.http_method = 'GET'
      GROUP BY b.region,l.file_format,b.query,b.bucket) d,
     stl_query q,
     (SELECT COUNT(1) AS cluster_slice_count FROM stv_slices)
WHERE a.tbl = b.oid
AND   b.relnamespace = c.oid
AND   d.query = a.query
AND   a.query = q.query
AND   lower(q.querytxt) LIKE '%copy %'
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 9 DESC, 21 DESC, 1 DESC
 LIMIT 50;
