drop table if exists tbl;

create table tbl (p Int64, t Int64, f Float64) Engine=MergeTree partition by p order by t settings index_granularity=1;

insert into tbl select number / 4, number, 0 from numbers(16);

select * from tbl WHERE indexHint(t = 1);

select * from tbl WHERE indexHint(t in (select toInt64(number) + 2 from numbers(3)));

select * from tbl WHERE indexHint(p = 2);

select * from tbl WHERE indexHint(p in (select toInt64(number) - 2 from numbers(3)));

drop table tbl;
