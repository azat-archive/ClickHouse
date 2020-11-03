drop table if exists data_01549;

set optimize_aggregation_in_order=1;
set read_in_order_two_level_merge_threshold=1;

create table data_01549 (
    key Int,
    key_hash Int alias key
) engine=MergeTree()
order by (key);
explain pipeline select key_hash from data_01549 group by key_hash;
drop table data_01549;

create table data_01549 (
    key Int,
    key_hash Int alias intHash64(key)
) engine=MergeTree()
order by (intHash64(key));
explain pipeline select key_hash from data_01549 group by key_hash;
drop table data_01549;
