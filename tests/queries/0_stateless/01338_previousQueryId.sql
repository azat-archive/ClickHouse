select 1 format Null; -- { queryId foobar }
select previousQueryId() == 'foobar';
select 1 format Null settings log_queries=0;
select previousQueryId() == 'foobar';
select 1 format Null;
select previousQueryId() != 'foobar';
