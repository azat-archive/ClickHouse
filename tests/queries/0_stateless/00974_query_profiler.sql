SET allow_introspection_functions = 1;

SET query_profiler_real_time_period_ns = 100000000;
SET log_queries = 1;
SELECT sleep(0.5); -- { queryId test-real-time-query-profiler }
SET log_queries = 0;
SYSTEM FLUSH LOGS;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = 'test-real-time-query-profiler' AND symbol LIKE '%FunctionSleep%';

SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
SET log_queries = 1;
SELECT count() FROM numbers(1000000000); -- { queryId test-cpu-time-query-profiler }
SET log_queries = 0;
SYSTEM FLUSH LOGS;

WITH addressToLine(arrayJoin(trace) AS addr) || '#' || demangle(addressToSymbol(addr)) AS symbol
SELECT count() > 0 FROM system.trace_log t WHERE query_id = 'test-cpu-time-query-profiler' AND symbol LIKE '%Source%';
