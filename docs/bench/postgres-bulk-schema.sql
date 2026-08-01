-- PostgreSQL half of docs/COMPARISON_POSTGRES_BULK.md: one large JSON corpus, no version history.
--
-- Distinct from postgres-comparison-schema.sql, which models a single small document with deep
-- version history. That experiment's corpus is 16 MiB and therefore entirely cache-resident
-- (COMPARISON_POSTGRES.md §0.14); this one is ~2 GB, which is what BENCHMARK_DESIGN.md §2.1 calls
-- the "buffer-pressured" regime: larger than shared_buffers and than any in-JVM cache.
--
-- Two representations, because they answer different questions:
--   movies_jsonb -- document parity, and the apples-to-apples arm. Same schemaless semantics
--                   SirixDB offers: every field of every record retained with no declared schema,
--                   and PostgreSQL parses JSON into a binary tree per row much as SirixDB shreds.
--   movies_rel   -- PostgreSQL at its best: a fixed relational schema with native arrays. Faster
--                   and smaller, but it needs the shape known up front plus an ETL pass SirixDB
--                   does not, so it is an upper bound rather than a like-for-like result.

DROP TABLE IF EXISTS movies_jsonb;
DROP TABLE IF EXISTS movies_rel;

CREATE TABLE movies_jsonb (
  id  bigint GENERATED ALWAYS AS IDENTITY,
  doc jsonb NOT NULL
);

CREATE TABLE movies_rel (
  title            text,
  year             int,
  cast_members     text[],
  genres           text[],
  href             text,
  extract          text,
  thumbnail        text,
  thumbnail_width  int,
  thumbnail_height int
);

-- Loading the jsonb arm.
--
-- COPY's text format treats backslash as an escape, so feeding it raw JSON corrupts every string
-- containing \" - the load fails on the first such record. CSV format with a quote and delimiter
-- that cannot occur in valid JSON (both are control characters; JSON escapes those as \uXXXX)
-- passes the bytes through untouched. Verify the corpus really lacks them before trusting this.
--
--   COPY movies_jsonb (doc) FROM '/path/corpus.ndjson'
--     WITH (FORMAT csv, QUOTE E'\x01', DELIMITER E'\x02');
--
-- Loading the normalized arm (tab-separated, COPY text format, \N for NULL):
--
--   COPY movies_rel FROM '/path/corpus.tsv';

-- The measured queries. Each is exactly equivalent to the SirixDB formulation in
-- PostgresBulkBench.QUERIES and returns a scalar that is cross-checked across all three arms.
--
--   countAll         SELECT count(*) FROM movies_jsonb;
--                    SELECT count(*) FROM movies_rel;
--   filterCountYear  SELECT count(*) FROM movies_jsonb WHERE (doc->>'year')::int > 1990;
--                    SELECT count(*) FROM movies_rel   WHERE year > 1990;
--   sumYear          SELECT sum((doc->>'year')::bigint) FROM movies_jsonb;
--                    SELECT sum(year::bigint)           FROM movies_rel;
--   titleLookup      SELECT count(*) FROM movies_jsonb WHERE doc->>'title' = 'Saleslady';
--                    SELECT count(*) FROM movies_rel   WHERE title = 'Saleslady';
