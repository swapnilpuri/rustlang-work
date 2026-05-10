-- Example query for ora2kinetica.
--
-- The placeholder {partition_clause} is REQUIRED -- the loader replaces it
-- per reader task with `ORA_HASH(<partition-column>, N-1) = TID`, where N is
-- --threads and TID is the task id (0..N-1). That way the N readers fetch
-- disjoint slices of the table concurrently, and Oracle filters them
-- server-side so only ~1/N of the rows ship over the wire.
--
-- Run with:
--   ora2kinetica --sql-file query.sql --partition-column ID ...

SELECT
    id,
    customer_id,
    amount,
    created_at,
    status
FROM transactions
WHERE {partition_clause}
  AND created_at >= DATE '2024-01-01'