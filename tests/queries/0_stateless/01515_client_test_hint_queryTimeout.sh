#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

# max_block_size=2 will allow query cancelation after 2 rows
# so total execution time should be 2 seconds and after client should cancel it due to queryTimeout.
start=$SECONDS
timeout 4s ${CLICKHOUSE_CLIENT} --testmode -nm --max_block_size=2 <<<'select throwIf(sleepEachRow(1) == 0) from numbers(10); -- { queryTimeout 1; }'
end=$SECONDS
echo $((end-start))
