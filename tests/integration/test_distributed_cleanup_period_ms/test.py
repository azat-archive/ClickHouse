# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long
# pylint: disable=broad-except

import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance('node',
    main_configs=['configs/config.d/distributed_cleanup_period_ms.xml'],
    user_configs=['configs/users.d/use_compact_format_in_distributed_parts_names.xml'],
)

@pytest.fixture(scope='module')
def start_cluster():
    try:
        cluster.start()
        node.query('CREATE DATABASE test ENGINE=Ordinary') # Different paths with Atomic
        yield cluster
    finally:
        cluster.shutdown()


def _has_distributed_directory(node, table, shard_num):
    try:
        node.exec_in_container(['ls', '-d', '/var/lib/clickhouse/data/test/{}/shard{}_replica1'.format(table, shard_num)])
        return True
    except Exception:
        # Exception for non successfull `ls` == directory does not exists
        # (any method except this try/except will be more complex, due to exec_in_container() realization)
        return False


def test_insert(start_cluster):
    node.query('CREATE TABLE test.foo (key Int) Engine=Memory()')
    node.query("""
    CREATE TABLE test.dist_foo (key Int)
    Engine=Distributed(
        test_cluster_two_shards,
        test,
        foo,
        key,
        'default'
    )
    """)
    # manual only (but only for remote node)
    node.query('SYSTEM STOP DISTRIBUTED SENDS test.dist_foo')

    assert not _has_distributed_directory(node, 'dist_foo', 1)
    assert not _has_distributed_directory(node, 'dist_foo', 2)

    node.query('INSERT INTO test.dist_foo SELECT * FROM numbers(100)')

    # shard1 -- localhost and since prefer_localhost_replica==1 it will not be created
    assert not _has_distributed_directory(node, 'dist_foo', 1)
    assert _has_distributed_directory(node, 'dist_foo', 2)

    assert node.query('SELECT count() FROM test.dist_foo') == '100\n'
    node.query('SYSTEM FLUSH DISTRIBUTED test.dist_foo')
    assert node.query('SELECT count() FROM test.dist_foo') == '200\n'

    # we need to enable distributed sends, otherwise cleanup worker will be no-op
    node.query('SYSTEM START DISTRIBUTED SENDS test.dist_foo')
    # wait enough time to schedule the cleanup worker
    time.sleep(5)
    assert not _has_distributed_directory(node, 'dist_foo', 1)
    assert not _has_distributed_directory(node, 'dist_foo', 2)
