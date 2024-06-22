WITH hour_list AS
(
  SELECT DATE_TRUNC('m',starttime) start_hour,
         dateadd('m',1,start_hour) AS end_hour
  FROM stl_query q
  WHERE starttime >= (getdate() -7)
  GROUP BY 1
),
scan_sum AS
(
  SELECT query,
         segment,
         SUM(bytes) AS bytes
  FROM stl_scan
  WHERE userid > 1
  GROUP BY query,
           segment
),
scan_list AS
(
  SELECT query,
         MAX(bytes) AS max_scan_bytes
  FROM scan_sum
  GROUP BY query
),
query_list AS
(
  SELECT w.query,
         exec_start_time,
         exec_end_time,
         ROUND(total_exec_time / 1000 / 1000.0,3) AS exec_sec,
         max_scan_bytes,
         CASE
           WHEN max_scan_bytes < 100000000 THEN 'small'
           WHEN max_scan_bytes BETWEEN 100000000 AND 500000000000 THEN 'medium'
           WHEN max_scan_bytes > 500000000000 THEN 'large'
         END AS size_type
  FROM stl_wlm_query w,
       scan_list sc
  WHERE sc.query = w.query
),
workload_exec_seconds  AS
(
select 
count(*) as query_cnt, 
SUM(CASE WHEN size_type = 'small' THEN  exec_sec  ELSE 0 END) AS small_workload_exec_sec_sum,
SUM(CASE WHEN size_type = 'medium' THEN  exec_sec  ELSE 0 END) AS medium_workload_exec_sec_sum,
SUM(CASE WHEN size_type = 'large' THEN  exec_sec  ELSE 0 END) AS large_workload_exec_sec_sum,
  
AVG(CASE WHEN size_type = 'small' THEN  exec_sec  ELSE 0 END) AS small_workload_exec_sec_avg,
AVG(CASE WHEN size_type = 'medium' THEN  exec_sec  ELSE 0 END) AS medium_workload_exec_sec_avg,
AVG(CASE WHEN size_type = 'large' THEN  exec_sec  ELSE 0 END) AS large_workload_exec_sec_avg,
  
MAX(CASE WHEN size_type = 'small' THEN  exec_sec  ELSE 0 END) AS small_workload_exec_sec_max,
MAX(CASE WHEN size_type = 'medium' THEN  exec_sec  ELSE 0 END) AS medium_workload_exec_sec_max,
MAX(CASE WHEN size_type = 'large' THEN  exec_sec  ELSE 0 END) AS large_workload_exec_sec_max,
  
  MIN(CASE WHEN size_type = 'small' THEN  exec_sec  ELSE 0 END) AS small_workload_exec_sec_min,
MIN(CASE WHEN size_type = 'medium' THEN  exec_sec  ELSE 0 END) AS medium_workload_exec_sec_min,
MIN(CASE WHEN size_type = 'large' THEN  exec_sec  ELSE 0 END) AS large_workload_exec_sec_min,
  
AVG(CASE WHEN size_type = 'small' THEN  max_scan_bytes  ELSE 0 END) AS small_workload_max_scan_bytes_avg,
AVG(CASE WHEN size_type = 'medium' THEN  max_scan_bytes  ELSE 0 END) AS medium_workload_max_scan_bytes_avg,
AVG(CASE WHEN size_type = 'large' THEN  max_scan_bytes  ELSE 0 END) AS large_workload_max_scan_bytes_avg,

  (small_workload_exec_sec_sum+medium_workload_exec_sec_sum+large_workload_exec_sec_sum) as total_workload_exec_sec_sum,
small_workload_exec_sec_sum/(total_workload_exec_sec_sum*1.00) as Small_workload_perc,
medium_workload_exec_sec_sum/(total_workload_exec_sec_sum*1.00) as Medium_workload_perc,
large_workload_exec_sec_sum/(total_workload_exec_sec_sum*1.00) as Large_workload_perc
from query_list
)
,query_list_2 AS
(
  SELECT start_hour,
         query,
         size_type,
         max_scan_bytes,
         exec_sec,
         exec_start_time,
         exec_end_time
  FROM hour_list h,
       query_list q
  WHERE exec_start_time BETWEEN start_hour AND end_hour
  OR    exec_end_time BETWEEN start_hour AND end_hour
  OR    (exec_start_time < start_hour AND exec_end_time > end_hour)
)

,hour_list_agg AS
(
  SELECT start_hour,
         SUM(CASE WHEN size_type = 'small' THEN 1 ELSE 0 END) AS small_query_cnt,
         SUM(CASE WHEN size_type = 'medium' THEN 1 ELSE 0 END) AS medium_query_cnt,
         SUM(CASE WHEN size_type = 'large' THEN 1 ELSE 0 END) AS large_query_cnt,
         COUNT(*) AS tot_query_cnt
  FROM query_list_2
  GROUP BY start_hour
) 
,utilization_perc AS
(
SELECT trunc(start_hour) AS sample_date,
       ROUND(100*SUM(CASE WHEN tot_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0,1) AS all_query_activite_perc,
       ROUND(100*SUM(CASE WHEN small_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0,1) AS small_query_activite_perc,
       ROUND(100*SUM(CASE WHEN medium_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0,1) AS medium_query_activite_perc,
       ROUND(100*SUM(CASE WHEN large_query_cnt > 0 THEN 1 ELSE 0 END) / 1440.0,1) AS large_query_activite_perc,
       MIN(start_hour) AS start_hour,
       MAX(start_hour) AS end_hour
FROM hour_list_agg
GROUP BY 1
)

,activity_perc as 
(
Select avg(small_query_activite_perc) AS AVG_small_query_activity_perc, 
avg(medium_query_activite_perc) AS AVG_medium_query_activity_perc,
avg(large_query_activite_perc) AS AVG_large_query_activity_perc
FROM utilization_perc
)

,mincount AS
(
SELECT trunc(start_hour) AS sample_date,
       SUM(CASE WHEN tot_query_cnt > 0 THEN 1 ELSE 0 END) AS tot_query_minute,
       SUM(CASE WHEN small_query_cnt > 0 THEN 1 ELSE 0 END) AS small_query_minute,
       SUM(CASE WHEN medium_query_cnt > 0 THEN 1 ELSE 0 END) AS medium_query_minute,
       SUM(CASE WHEN large_query_cnt > 0 THEN 1 ELSE 0 END) AS large_query_minute,
       MIN(start_hour) AS start_hour,
       MAX(start_hour) AS end_hour
FROM hour_list_agg
GROUP BY 1
),avgmincount AS
(
Select avg(small_query_minute) avg_small_query_minute, avg(medium_query_minute) avg_medium_query_minute, avg(large_query_minute) avg_large_query_minute
  from mincount
)
,final_output AS 
(
Select   small_workload_perc , medium_workload_perc,    large_workload_perc,    
  avg_small_query_activity_perc,    avg_medium_query_activity_perc,    avg_large_query_activity_perc,    
  avg_small_query_minute,    avg_medium_query_minute,    avg_large_query_minute,
  
  small_workload_exec_sec_avg, medium_workload_exec_sec_avg, large_workload_exec_sec_avg,
  small_workload_exec_sec_max, medium_workload_exec_sec_max, large_workload_exec_sec_max,
  small_workload_exec_sec_min, medium_workload_exec_sec_min, large_workload_exec_sec_min,
   total_query_cnt, total_small_query_cnt, 
  total_medium_query_cnt,total_large_query_cnt, 
  small_workload_max_scan_bytes_avg, 
  medium_workload_max_scan_bytes_avg, 
  large_workload_max_scan_bytes_avg
from activity_perc a, avgmincount b, workload_exec_seconds c, (  select count(*) as total_query_cnt, sum(case when size_type = 'small' then 1 else 0 end) as  total_small_query_cnt, 
                                                              sum(case when size_type = 'medium' then 1 else 0 end) as  total_medium_query_cnt, 
                                                              sum(case when size_type = 'large' then 1 else 0 end) as  total_large_query_cnt
                                                              from query_list )  d
where 1=1

)
select workloadtype, (perc_of_total_workload*100.00) as perc_of_total_workload, perc_duration_in_day, Total_query_minutes_in_day 
,workload_exec_sec_avg, workload_exec_sec_min, workload_exec_sec_max,query_cnt,scan_bytes_avg
from
(
select 
'Small' as workloadtype,
small_workload_perc as perc_of_total_workload,
avg_small_query_activity_perc as perc_duration_in_day,
avg_small_query_minute as Total_query_minutes_in_day, 
 small_workload_exec_sec_avg as  workload_exec_sec_avg,
  small_workload_exec_sec_max as  workload_exec_sec_max, 
   small_workload_exec_sec_min as  workload_exec_sec_min,
 total_small_query_cnt as query_cnt, 
  small_workload_max_scan_bytes_avg as scan_bytes_avg,
 1 as id
from final_output
union
select 
'Meduim' as workloadtype,
medium_workload_perc as perc_of_total_workload,
avg_medium_query_activity_perc as perc_duration_in_day,
avg_medium_query_minute as Total_query_minutes_in_day ,
  medium_workload_exec_sec_avg as  workload_exec_sec_avg,
    medium_workload_exec_sec_max as  workload_exec_sec_max, 
   medium_workload_exec_sec_min as  workload_exec_sec_min,
 total_medium_query_cnt as query_cnt, 
  medium_workload_max_scan_bytes_avg as scan_bytes_avg,
  2 as id
from final_output
union
select 
'Large' as workloadtype,
large_workload_perc as perc_of_total_workload,
avg_large_query_activity_perc as perc_duration_in_day,
avg_large_query_minute as Total_query_minutes_in_day, 
  large_workload_exec_sec_avg as  workload_exec_sec_avg,
   large_workload_exec_sec_max as  workload_exec_sec_max, 
   large_workload_exec_sec_min as  workload_exec_sec_min,
 total_large_query_cnt as query_cnt, 
  large_workload_max_scan_bytes_avg as scan_bytes_avg,
  3 as id
 from final_output
  ) a order by id asc;
