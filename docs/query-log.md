# Query optimization log

Record every non-trivial query here: the problem, the `EXPLAIN ANALYZE` before, the fix (index/partition/rewrite), and the `EXPLAIN ANALYZE` after. This file is what turns "I used PostgreSQL" into a resume bullet with a number in it.

## Station hourly-demand history lookup

- **Query:**

  ```sql
  SELECT hour, departures, arrivals, net_flow, temperature_c, precipitation_mm
  FROM station_hourly_demand
  WHERE station_id = '6364.07'
  ORDER BY hour;
  ```

  The natural "one station's demand history" query — what a `/stations/{id}/history`-style
  API endpoint or the Phase 6 risk-prediction work would run. `station_hourly_demand` is
  765,023 rows (2,272 stations × ~28 days × 24h for February 2025); a single station's
  history is ~642 rows, ~0.08% of the table.

- **Before:** no index on `station_hourly_demand` — the dbt mart model had none. `EXPLAIN
  (ANALYZE, BUFFERS)`:

  ```
  Gather Merge  (actual time=41.792..43.947 rows=642 loops=1)
    Workers Planned: 2
    Workers Launched: 2
    Buffers: shared hit=336 read=7648
    ->  Sort  (actual time=38.984..39.002 rows=214 loops=3)
          Sort Key: hour
          Sort Method: quicksort  Memory: 41kB
          ->  Parallel Seq Scan on station_hourly_demand  (actual time=19.719..38.823 rows=214 loops=3)
                Filter: (station_id = '6364.07'::text)
                Rows Removed by Filter: 254794
                Buffers: shared hit=224 read=7648
  Execution Time: 44.062 ms
  ```

  A parallel sequential scan reads essentially the entire table (~7,984 buffer accesses)
  to throw away 254,794 non-matching rows across workers, then sorts the 642 survivors
  separately since nothing provides pre-sorted order.

- **Problem diagnosed:** no index covers the filter column (`station_id`) or the sort
  column (`hour`), forcing a full-table parallel scan plus a standalone sort for every
  lookup of a single station's history.

- **Fix applied:** a composite btree index on `(station_id, hour)` — covers the equality
  filter and gives the `ORDER BY` for free (no separate sort node needed). Declared in the
  dbt model's `config()` block rather than added by hand via `psql`: `station_hourly_demand`
  is `materialized: table`, so a bare `CREATE INDEX` would be silently dropped the next
  time `dbt run` rebuilds the table. `dbt/models/marts/station_hourly_demand.sql`:

  ```sql
  {{ config(
      indexes=[
          {'columns': ['station_id', 'hour'], 'type': 'btree'},
      ]
  ) }}
  ```

  One methodology note from getting to the final number: the first `EXPLAIN ANALYZE` run
  immediately after `dbt run` rebuilt the table showed a `Bitmap Heap Scan` with a wildly
  off row estimate (3,825 planned vs. 642 actual) — `dbt run`'s `CREATE TABLE AS` doesn't
  auto-`ANALYZE`, so the planner was working off stale/default statistics for the
  freshly-rebuilt table. Running `ANALYZE station_hourly_demand;` fixed the estimate and
  is what produced the plain `Index Scan` below. Worth remembering for any table dbt
  rebuilds from scratch: run `ANALYZE` (or rely on autovacuum's analyze threshold, which
  will eventually catch it) before trusting a plan on it.

- **After:** `EXPLAIN (ANALYZE, BUFFERS)` post-index, post-`ANALYZE`:

  ```
  Index Scan using "7a7d0f70ad3e9b417ae81ae785300d39" on station_hourly_demand
    (actual time=0.020..0.151 rows=642 loops=1)
    Index Cond: (station_id = '6364.07'::text)
    Buffers: shared hit=13
  Execution Time: 0.222 ms
  ```

  Straight index scan returning already-sorted rows — no separate sort step, 13 buffer
  hits (fully cached, zero disk reads) instead of ~7,984.

- **Speedup:** 44.062 ms → 0.222 ms, **~198x**. Buffer accesses: ~7,984 → 13.

## Template

### <short title>

- **Query:**
- **Before:** `EXPLAIN ANALYZE` output, execution time
- **Problem diagnosed:**
- **Fix applied:**
- **After:** `EXPLAIN ANALYZE` output, execution time
- **Speedup:**
