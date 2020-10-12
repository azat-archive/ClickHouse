#!/usr/bin/env bash

# XXX: cannot use expect since we need two clients
# XXX: requires bash 4.4+

function assert()
{
    if ! "$@"; then
        echo "'$*' failed" >&2
        exit 1
    fi
}

rm -f history
rm -f client1.txt
rm -f client2.txt
export CLICKHOUSE_HISTORY_FILE=.history

# Wrap with script to make stdin terminal, to activate replxx
coproc client1 { clickhouse-client client1.txt; }
# Will produce warning, but should work:
#     warning: execute_coproc: coproc [3418021:client1] still exists
coproc client2 { clickhouse-client client2.txt; }

# save fds, to avoid exec for each echo
exec 10>&${client1[1]}-
exec 20>&${client2[1]}-

# regular history
echo "SELECT 'first_line '" >&10
echo "SELECT 'second_line'" >&20

# wait until the history will be dumped
sleep 2

assert grep -F -q first_line  history
assert grep -F -q second_line history

# dump history
echo .history >&10
echo .history >&20

# wait until
# - client1.txt
# - client2.txt
sleep 0.1

assert test $(grep -c -F second_line client1.txt) -eq 0
assert test $(grep -c -F first_line  client2.txt) -eq 0

# quit
echo .quit >&10
echo .quit >&20

wait $client1_PID
wait $client2_PID

echo OK
