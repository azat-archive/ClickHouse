#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

opts=(
    "--tcp_port=19000"
    "--http_port=18123"

    "--shutdown_wait_unfinished=0"
)
CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY -- "${opts[@]}" >& clickhouse-server.log &
server_pid=$!

# wait for the server to start accepting tcp connections
while ! $CLICKHOUSE_CLIENT_BINARY --port 19000 --format Null -q 'select 1' 2>/dev/null; do
    :
done

query_id="$CLICKHOUSE_DATABASE-$SECONDS"
$CLICKHOUSE_CLIENT_BINARY --query_id "$query_id" --port 19000 --format Null -q 'select sleepEachRow(1) from numbers(10)' 2>/dev/null &
client_pid=$!

trap cleanup EXIT
function cleanup()
{
    kill -9 $server_pid
    kill -9 $client_pid

    echo "Server log"
    cat clickhouse-server.log
    rm -f clickhouse-server.log

    exit 1
}

# wait until the query will appear in processlist
# (it is enough to trigger the problem)
while [[ $($CLICKHOUSE_CLIENT_BINARY --port 19000 -q "select count() from system.processes where query_id = '$query_id'") != "1" ]]; do
    :
done

# send TERM and save the error code to ensure that it is 0 (EXIT_SUCCESS)
kill $server_pid
wait $server_pid
return_code=$?

wait $client_pid

trap '' EXIT
if [ $return_code != 0 ]; then
    cat clickhouse-server.log
fi
rm -f clickhouse-server.log

exit $return_code
