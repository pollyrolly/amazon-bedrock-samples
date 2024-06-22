select date_trunc('hour',end_time) as burst_hour
      ,sum(queries) as query_count
      ,sum(usage_in_seconds) as burst_usage_in_seconds  
from svcs_concurrency_scaling_usage
group by burst_hour;