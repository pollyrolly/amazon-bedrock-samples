with workload as
(
select trim(sq."database") as dbname
      ,case 
	     when sq.concurrency_scaling_status = 1 then 'burst'	        
			 else 'main' end as concurrency_scaling_status 
      ,case 
       when sl.source_query is not null then 'result_cache'       
			 else rtrim(swsc.name) end as queue_name 
			,swq.service_class 
      ,case
       when swq.service_class between 1 and 4 then 'System'
       when swq.service_class = 5 then 'Superuser'
       when swq.service_class between 6 and 13 then'Manual WLM queues'
       when swq.service_class = 14 then 'SQA'
       when swq.service_class = 15 then 'Redshift Maintenance'
       when swq.service_class between 100 and 107 then 'Auto WLM'
       end as service_class_category 	  
      ,sq.query as query_id
      ,case 
         when regexp_instr(sq.querytxt, '(padb_|pg_internal)'             ) then 'OTHER'
         when regexp_instr(sq.querytxt, '([uU][nN][dD][oO][iI][nN][gG]) ' ) then 'SYSTEM'
         when regexp_instr (sq.querytxt,'([aA][uU][tT][oO][mM][vV])'      ) then 'AUTOMV'
         when regexp_instr(sq.querytxt, '[uU][nN][lL][oO][aA][dD]'        ) then 'UNLOAD'
         when regexp_instr(sq.querytxt, '[cC][uU][rR][sS][oO][rR] '       ) then 'CURSOR'
         when regexp_instr(sq.querytxt, '[fF][eE][tT][cC][hH] '           ) then 'CURSOR'
         WHEN regexp_instr (sq.querytxt,'[cC][rR][eE][aA][tT][eE] '       ) then 'CTAS'
         when regexp_instr(sq.querytxt, '[dD][eE][lL][eE][tT][eE] '       ) then 'DELETE'
         when regexp_instr(sq.querytxt, '[uU][pP][dD][aA][tT][eE] '       ) then 'UPDATE'
         when regexp_instr(sq.querytxt, '[iI][nN][sS][eE][rR][tT] '       ) then 'INSERT'
         when regexp_instr(sq.querytxt, '[vV][aA][cC][uU][uU][mM][ :]'    ) then 'VACUUM'
         when regexp_instr(sq.querytxt, '[aA][nN][aA][lL][yY][zZ][eE] '   ) then 'ANALYZE'		 
         when regexp_instr(sq.querytxt, '[sS][eE][lL][eE][cC][tT] '       ) then 'SELECT'
         when regexp_instr(sq.querytxt, '[cC][oO][pP][yY] '               ) then 'COPY'
         else 'OTHER' 
       end as query_type 
      ,date_trunc('hour',sq.starttime) as workload_exec_hour
      ,nvl(swq.est_peak_mem/1024.0/1024.0/1024.0,0.0) as est_peak_mem_gb
      ,decode(swq.final_state, 'Completed',decode(swr.action, 'abort',0,decode(sq.aborted,0,1,0)),'Evicted',0,null,decode(sq.aborted,0,1,0)::int) as is_completed
      ,decode(swq.final_state, 'Completed',decode(swr.action, 'abort',1,0),'Evicted',1,null,0) as is_evicted_aborted
      ,decode(swq.final_state, 'Completed',decode(swr.action, 'abort',0,decode(sq.aborted,1,1,0)),'Evicted',0,null,decode(sq.aborted,1,1,0)::int) as is_user_aborted
	    ,case when sl.from_sp_call is not null then 1 else 0 end as from_sp_call
	    ,case when alrt.num_events is null then 0 else alrt.num_events end as alerts
	    ,case when dsk.num_diskbased > 0 then 1 else 0 end as is_query_diskbased
	    ,nvl(c.num_compile_segments,0) as num_compile_segments
      ,cast(case when sqms.query_queue_time is null then 0 else sqms.query_queue_time end as decimal(26,6)) as query_queue_time_secs
	    ,nvl(c.max_compile_time_secs,0) as max_compile_time_secs
	    ,sl.starttime
	    ,sl.endtime
	    ,sl.elapsed
      ,cast(sl.elapsed * 0.000001 as decimal(26,6)) as query_execution_time_secs	
      ,sl.elapsed * 0.000001 - nvl(c.max_compile_time_secs,0)  - nvl(sqms.query_queue_time,0) as actual_execution_time_secs	  
      ,case when sqms.query_temp_blocks_to_disk is null then 0 else sqms.query_temp_blocks_to_disk end as query_temp_blocks_to_disk_mb
      ,cast(case when sqms.query_cpu_time is null then 0 else sqms.query_cpu_time end as decimal(26,6)) as query_cpu_time_secs 
	    ,nvl(sqms.scan_row_count,0) as scan_row_count
      ,nvl(sqms.return_row_count,0) as return_row_count
      ,nvl(sqms.nested_loop_join_row_count,0) as nested_loop_join_row_count
      ,nvl(uc.usage_limit_count,0) as cs_usage_limit_count
  from stl_query sq
  inner join svl_qlog sl on (sl.userid = sq.userid and sl.query = sq.query)
  left outer join svl_query_metrics_summary sqms on (sqms.userid = sq.userid and sqms.query = sq.query)					
  left outer join stl_wlm_query swq on (sq.userid = swq.userid and sq.query = swq.query)
  left outer join stl_wlm_rule_action swr on (sq.userid = swr.userid and sq.query = swr.query and swq.service_class = swr.service_class)
  left outer join stv_wlm_service_class_config swsc on (swsc.service_class = swq.service_class)
  left outer join (select sae.query
                         ,cast(1 as integer) as num_events
                     from svcs_alert_event_log sae
                   group by sae.query) as alrt on (alrt.query = sq.query)  
  left outer join (select sqs.userid
                         ,sqs.query
                         ,1 as num_diskbased
                     from svcs_query_summary sqs    
                    where sqs.is_diskbased = 't'
                   group by sqs.userid, sqs.query
                   ) as dsk on (dsk.userid = sq.userid and dsk.query = sq.query)  
  left outer join (select userid, xid,  pid, query
                         ,max(datediff(ms, starttime, endtime)*1.0/1000) as max_compile_time_secs
	                     ,sum(compile) as num_compile_segments
                     from svcs_compile
                   group by userid, xid,  pid, query
                  ) c on (c.userid = sq.userid and c.xid = sq.xid and c.pid = sq.pid and c.query = sq.query)                 
  left outer join (select query,xid,pid
                          ,count(1) as usage_limit_count
                      from stl_usage_control 
                     where feature_type = 'CONCURRENCY_SCALING'
                   group by query, xid, pid) uc on (uc.xid = sq.xid and uc.pid = sq.pid and uc.query = sq.query)                  	   
  where sq.userid <> 1 
    and sq.querytxt not like 'padb_fetch_sample%'
    and sq.starttime >= dateadd(day,-7,current_date)
)
select workload_exec_hour
      ,service_class_category
      ,service_class
      ,queue_name
      ,concurrency_scaling_status
      ,dbname
      ,query_type
      ,sum(is_completed) + sum(is_user_aborted) + sum(is_evicted_aborted) as total_query_count
      ,sum(is_completed) as completed_query_count
      ,sum(is_user_aborted) as user_aborted_count
      ,sum(is_evicted_aborted) as wlm_evicted_count
      ,round(sum(est_peak_mem_gb),4) as total_est_peak_mem_gb
      ,sum(is_query_diskbased) as total_disk_spill_count
      ,sum(num_compile_segments) as total_compile_count
      ,round(sum(query_temp_blocks_to_disk_mb/1024.0),4) as total_disk_spill_gb
      ,sum(alerts) as total_query_alert_count
      ,sum(from_sp_call) as total_called_proc_count
	  ,avg(query_execution_time_secs) as avg_query_execution_time_secs
	  ,max(query_execution_time_secs) as max_query_execution_time_secs
      ,sum(query_execution_time_secs) as total_query_execution_time_secs
	  ,avg(max_compile_time_secs) as avg_compile_time_secs
	  ,max(max_compile_time_secs) as max_compile_time_secs
      ,sum(max_compile_time_secs) as total_compile_time_secs
	  ,avg(query_queue_time_secs) as avg_query_queue_time_secs
	  ,max(query_queue_time_secs) as max_query_queue_time_secs
      ,sum(query_queue_time_secs) as total_query_queue_time_secs
	  ,avg(actual_execution_time_secs) as avg_actual_execution_time_secs
      ,sum(actual_execution_time_secs) as total_actual_execution_time_secs
      ,sum(query_cpu_time_secs) as total_query_cpu_time_secs
      ,sum(cs_usage_limit_count) as total_cs_usage_limit_count
      ,sum(scan_row_count) as total_scan_row_count
      ,sum(return_row_count) as total_return_row_count
      ,sum(nested_loop_join_row_count) as total_nl_join_row_count
  from workload
  group by 1,2,3,4,5,6,7
  order by 1,2,3,4,5,6,7;