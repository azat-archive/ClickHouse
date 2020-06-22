SET allow_introspection_functions = 1;

SET memory_profiler_step = 1000000;
SELECT groupArray(number) FROM numbers(10000000); -- { queryId test-memory-profiler }
SYSTEM FLUSH LOGS;

WITH addressToSymbol(arrayJoin(trace)) AS symbol
SELECT count() > 0
FROM system.trace_log t
WHERE event_date >= yesterday() AND trace_type = 'Memory' AND query_id = 'test-memory-profiler';
