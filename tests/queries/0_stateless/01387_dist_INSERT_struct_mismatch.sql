drop table if exists dist_01387;
drop table if exists dist_layer_01387;
drop table if exists data_01387;

create table data_01387 (key SimpleAggregateFunction(max, Nullable(Int))) Engine=Memory();
create table dist_layer_01387 (key Nullable(Int)) Engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01387, key);
create table dist_01387 (key Int) Engine=Distributed(test_cluster_two_shards, currentDatabase(), dist_layer_01387, key);

insert into dist_01387 values (1, 2);
system flush distributed dist_01387;
system flush distributed dist_layer_01387;

select * from data_01387;
