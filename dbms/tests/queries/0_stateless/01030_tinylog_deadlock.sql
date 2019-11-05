DROP TABLE IF EXISTS local;
DROP TABLE IF EXISTS distributed;

SET insert_distributed_sync=1;

CREATE TABLE local (date Date, value Date MATERIALIZED toDate('2017-08-01')) ENGINE = TinyLog();
CREATE TABLE distributed AS local ENGINE = Distributed('test_cluster_two_remote_shards', currentDatabase(), local, rand());

INSERT INTO distributed VALUES ('2018-08-01');

SELECT * FROM distributed;
SELECT date, value FROM distributed;
SELECT * FROM local;
SELECT date, value FROM local;

DROP TABLE distributed;
DROP TABLE local;
