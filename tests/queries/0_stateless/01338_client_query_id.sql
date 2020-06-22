set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

-- initial
select 1 format Null; -- { queryId 01338_client_query_id }
system flush logs;
select count() from system.query_log where query_id = '01338_client_query_id';

-- no queryId
select 1 format Null;
system flush logs;
select count() from system.query_log where query_id = '01338_client_query_id';

-- overwrite
select 1 format Null; -- { queryId 01338_client_query_id }
system flush logs;
select count() from system.query_log where query_id = '01338_client_query_id';

-- another queryId
select 1 format Null; -- { queryId 01338_client_query_id_2 }
system flush logs;
select count() from system.query_log where query_id = '01338_client_query_id_2';
