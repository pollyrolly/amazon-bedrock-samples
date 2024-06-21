with consumer_activity as 
(
select uc.userid
      ,u.usename as db_username
      ,uc.pid
      ,uc.xid
      ,min(uc.recordtime) as request_start_date
      ,max(uc.recordtime) as request_end_date
      ,datediff('milliseconds',min(uc.recordtime),max(uc.recordtime))::NUMERIC(38,4) / 1000 as request_duration_secs
      ,nvl(count(distinct uc.transaction_uid),0) as unique_transaction
      ,nvl(count(uc.request_id),0) as total_usage_consumer_count
      ,sum(case when trim(uc.error) = '' then 0 else 1 end) as request_error_count
  from svl_datashare_usage_consumer uc
  inner join pg_user u on (u.usesysid = uc.userid)
group by 1,2,3,4
)
,consumer_query as (
select trim(q."database") as dbname
      ,trim(cu.db_username) as db_username
      ,cu.request_start_date::date as request_date
      ,cu.request_duration_secs
      ,datediff('milliseconds',cu.request_end_date,q.starttime)::NUMERIC(38,4) / 1000  as request_interval_secs
      ,datediff('milliseconds',q.starttime,q.endtime)::NUMERIC(38,4) / 1000 as query_execution_secs
      ,datediff('milliseconds',request_start_date,q.endtime)::NUMERIC(38,4) / 1000 as total_execution_secs
      ,q.query
      ,cu.unique_transaction
      ,cu.total_usage_consumer_count
      ,cu.request_error_count      
from consumer_activity cu 
inner join stl_query q on (q.xid = cu.xid and q.pid = cu.pid and q.userid = cu.userid)
)
,consumer_query_aggregate as (
select cq.request_date
      ,cq.dbname
      ,cq.db_username
	  ,avg(cq.request_duration_secs) as avg_request_duration_secs
      ,sum(cq.request_duration_secs) as total_request_duration_secs
	  ,avg(cq.request_interval_secs) as avg_request_interval_secs
      ,sum(cq.request_interval_secs) as total_request_interval_secs
	  ,avg(cq.query_execution_secs) as avg_query_execution_secs
      ,sum(cq.query_execution_secs) as total_query_execution_secs
	  ,avg(cq.total_execution_secs) as avg_execution_secs
      ,sum(cq.total_execution_secs) as total_execution_secs
      ,count(cq.query) as query_count
      ,sum(cq.unique_transaction) as total_unique_transaction
      ,sum(cq.total_usage_consumer_count) as total_usage_consumer_count
      ,sum(cq.request_error_count) as total_request_error_count 
  from consumer_query cq
group by 1,2,3
)
,consumer_query_request_percentile AS (
SELECT cq.request_date
      ,cq.dbname
      ,cq.db_username
	  ,percentile_cont(0.8) within GROUP ( ORDER BY request_duration_secs) AS p80_request_sec
	  ,percentile_cont(0.9) within GROUP ( ORDER BY request_duration_secs) AS p90_request_sec
	  ,percentile_cont(0.99) within GROUP ( ORDER BY request_duration_secs) AS p99_request_sec
  from consumer_query cq
group by 1,2,3
)
select cqa.request_date
      ,cqa.dbname
      ,cqa.db_username
      ,cqa.query_count
	  ,cqa.avg_query_execution_secs
      ,cqa.total_query_execution_secs
	  ,cqa.avg_execution_secs
      ,cqa.total_execution_secs
	  ,cqa.avg_request_duration_secs
	  ,cqrp.p80_request_sec
	  ,cqrp.p90_request_sec
	  ,cqrp.p99_request_sec
      ,cqa.total_request_duration_secs
	  ,cqa.avg_request_interval_secs
      ,cqa.total_request_interval_secs	  
      ,cqa.total_unique_transaction
      ,cqa.total_usage_consumer_count
      ,cqa.total_request_error_count
  from consumer_query_aggregate cqa
  inner join consumer_query_request_percentile cqrp on (cqa.request_date = cqrp.request_date and cqa.dbname = cqrp.dbname and cqa.db_username = cqrp.db_username)
order by 1,2,3;
