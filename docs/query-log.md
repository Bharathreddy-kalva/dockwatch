# Query optimization log

Record every non-trivial query here: the problem, the `EXPLAIN ANALYZE` before, the fix (index/partition/rewrite), and the `EXPLAIN ANALYZE` after. This file is what turns "I used PostgreSQL" into a resume bullet with a number in it.

## Template

### <short title>

- **Query:**
- **Before:** `EXPLAIN ANALYZE` output, execution time
- **Problem diagnosed:**
- **Fix applied:**
- **After:** `EXPLAIN ANALYZE` output, execution time
- **Speedup:**
