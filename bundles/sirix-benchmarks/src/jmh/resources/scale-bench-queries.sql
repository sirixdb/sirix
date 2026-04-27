-- SQL equivalents of the BrackitQueryOnSirixScaleMain benchmark suite.
-- Load the CSV first:
--   CREATE TABLE t AS SELECT * FROM read_csv_auto('/tmp/records-10m.csv');
-- Or tighten to Parquet+Zstd:
--   COPY t TO '/tmp/records-10m.parquet' (FORMAT 'parquet', COMPRESSION 'zstd');
--   CREATE TABLE t AS SELECT * FROM '/tmp/records-10m.parquet';

--============================================================
-- filterCount — count(for $u where $u.age > 40 and $u.active)
--============================================================
SELECT count(*) FROM t WHERE age > 40 AND active;

--============================================================
-- groupByDept — let $d := dept group by $d return {dept, count}
--============================================================
SELECT dept, count(*) FROM t GROUP BY dept ORDER BY dept;

--============================================================
-- sumAge — sum of $u.age
--============================================================
SELECT sum(age) FROM t;

--============================================================
-- avgAge
--============================================================
SELECT avg(age) FROM t;

--============================================================
-- minMaxAge
--============================================================
SELECT min(age) AS min, max(age) AS max FROM t;

--============================================================
-- groupBy2Keys — group by dept, city
--============================================================
SELECT dept, city, count(*) FROM t GROUP BY dept, city ORDER BY dept, city;

--============================================================
-- filterGroupBy — where active group by dept
--============================================================
SELECT dept, count(*) FROM t WHERE active GROUP BY dept ORDER BY dept;

--============================================================
-- countDistinct — count distinct dept
--============================================================
SELECT count(DISTINCT dept) FROM t;

--============================================================
-- compoundAndFilterCount — age > 30 AND age < 50 AND active
--============================================================
SELECT count(*) FROM t WHERE age > 30 AND age < 50 AND active;

--============================================================
-- filterGroupByAge — where age > 40 group by dept
--============================================================
SELECT dept, count(*) FROM t WHERE age > 40 GROUP BY dept ORDER BY dept;

--============================================================
-- Timing — wrap each query with EXPLAIN ANALYZE or .timer on in DuckDB:
--   .timer on
--   SELECT ...;
--============================================================
