-- NOTE: this test cannot use 'current_database = currentDatabase()',
-- because it does not propagated via remote queries,
-- hence it uses 'with (select currentDatabase()) as X'
-- (with subquery to expand it on the initiator).

drop table if exists dist_01756;
create table dist_01756 as system.one engine=Distributed(test_cluster_two_shards, system, one, dummy);

-- separate log entry for localhost queries
set prefer_localhost_replica=0;
set force_optimize_skip_unused_shards=2;
set optimize_skip_unused_shards=1;
set optimize_skip_unused_shards_rewrite_in=0;
set log_queries=1;

--
-- w/o optimize_skip_unused_shards_rewrite_in=1
--
select '(0, 1)';
with (select currentDatabase()) as id_no select *, ignore(id_no) from dist_01756 where dummy in (0, 1);
system flush logs;
select query from system.query_log where
    event_date = today() and
    event_time > now() - interval 1 hour and
    not is_initial_query and
    query not like '%system.query_log%' and
    query like concat('WITH%', currentDatabase(), '%AS id_no %') and
    type = 'QueryFinish'
order by query;

--
-- w/ optimize_skip_unused_shards_rewrite_in=1
--

set optimize_skip_unused_shards_rewrite_in=1;

-- detailed coverage for realistic examples
select 'optimize_skip_unused_shards_rewrite_in(0, 1)';
with (select currentDatabase()) as id_01 select *, ignore(id_01) from dist_01756 where dummy in (0, 1);
system flush logs;
select query from system.query_log where
    event_date = today() and
    event_time > now() - interval 1 hour and
    not is_initial_query and
    query not like '%system.query_log%' and
    query like concat('WITH%', currentDatabase(), '%AS id_01 %') and
    type = 'QueryFinish'
order by query;

select 'optimize_skip_unused_shards_rewrite_in(1,)';
with (select currentDatabase()) as id_1 select *, ignore(id_1) from dist_01756 where dummy in (1,);
system flush logs;
select query from system.query_log where
    event_date = today() and
    event_time > now() - interval 1 hour and
    not is_initial_query and
    query not like '%system.query_log%' and
    query like concat('WITH%', currentDatabase(), '%AS id_1 %') and
    type = 'QueryFinish'
order by query;

select 'optimize_skip_unused_shards_rewrite_in(0,)';
with (select currentDatabase()) as id_0 select *, ignore(id_0) from dist_01756 where dummy in (0,);
system flush logs;
select query from system.query_log where
    event_date = today() and
    event_time > now() - interval 1 hour and
    not is_initial_query and
    query not like '%system.query_log%' and
    query like concat('WITH%', currentDatabase(), '%AS id_0 %') and
    type = 'QueryFinish'
order by query;

--
-- errors
--
select 'errors';

-- not tuple
select * from dist_01756 where dummy in (0); -- { serverError 507 }
-- optimize_skip_unused_shards does not support non-constants
select * from dist_01756 where dummy in (select * from system.one); -- { serverError 507 }
select * from dist_01756 where dummy in (toUInt8(0)); -- { serverError 507 }
-- wrong type
select * from dist_01756 where dummy in ('0'); -- { serverError 507 }
-- NOT IN does not supported
select * from dist_01756 where dummy not in (0, 1); -- { serverError 507 }

--
-- others
--
select 'others';

select * from dist_01756 where dummy not in (2, 3) and dummy in (0, 1);
select * from dist_01756 where dummy in tuple(0, 1);
select * from dist_01756 where dummy in tuple(0);
select * from dist_01756 where dummy in tuple(1);
-- Identifier is NULL
select (1 IN (1,)), * from dist_01756 where dummy in (0, 1) format Null;
-- Literal is NULL
select (dummy IN (toUInt8(1),)), * from dist_01756 where dummy in (0, 1) format Null;
