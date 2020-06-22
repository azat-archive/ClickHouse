set log_queries=1;

select 1; -- { queryId 01231_log_queries_min_type-QUERY_START }
system flush logs;
select count() from system.query_log where query_id = '01231_log_queries_min_type-QUERY_START' and event_date = today() and event_time >= now() - interval 1 minute;

set log_queries_min_type='EXCEPTION_BEFORE_START';
select 1; -- { queryId 01231_log_queries_min_type-EXCEPTION_BEFORE_START }
system flush logs;
select count() from system.query_log where query_id = '01231_log_queries_min_type-EXCEPTION_BEFORE_START' and event_date = today() and event_time >= now() - interval 1 minute;

set log_queries_min_type='EXCEPTION_WHILE_PROCESSING';
select max(number) from system.numbers limit 1e6 settings max_rows_to_read='100K'; -- { serverError 158, queryId 01231_log_queries_min_type-EXCEPTION_WHILE_PROCESSING }
system flush logs;
select count() from system.query_log where query_id = '01231_log_queries_min_type-EXCEPTION_WHILE_PROCESSING' and event_date = today() and event_time >= now() - interval 1 minute;
