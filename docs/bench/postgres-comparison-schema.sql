-- PostgreSQL half of docs/COMPARISON_POSTGRES.md (W1-W6).
-- Mirrors the SirixDB workload: one document whose full version history is retained.
DROP TABLE IF EXISTS doc_history;
DROP TABLE IF EXISTS doc;

CREATE TABLE doc (
  id  int PRIMARY KEY,
  doc jsonb NOT NULL
);

CREATE TABLE doc_history (
  id         int         NOT NULL,
  rev        bigserial   PRIMARY KEY,
  valid_from timestamptz NOT NULL DEFAULT clock_timestamp(),
  doc        jsonb       NOT NULL
);

CREATE INDEX doc_history_valid_from_idx ON doc_history (valid_from);

-- History is maintained IN THE SAME TRANSACTION as the update, so a committed version and its
-- history row are durable together — the same guarantee SirixDB's commit gives by construction.
CREATE OR REPLACE FUNCTION doc_history_trigger() RETURNS trigger AS $$
BEGIN
  INSERT INTO doc_history (id, doc) VALUES (NEW.id, NEW.doc);
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER doc_history_trg
  AFTER INSERT OR UPDATE ON doc
  FOR EACH ROW EXECUTE FUNCTION doc_history_trigger();

-- W1: n single-field updates, each its own durable transaction. Server-side so PostgreSQL pays no
-- client round trip per commit — the most favourable honest framing for it (see caveat #1).
CREATE OR REPLACE PROCEDURE bench_w1(n int) AS $$
DECLARE i int;
BEGIN
  FOR i IN 1..n LOOP
    UPDATE doc SET doc = jsonb_set(doc, '{counter}', to_jsonb(i)) WHERE id = 1;
    COMMIT;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- W2: random point-in-time reads, each fetching AND serializing the whole document.
CREATE OR REPLACE FUNCTION bench_w2(n int) RETURNS bigint AS $$
DECLARE
  lo   timestamptz;
  hi   timestamptz;
  t    timestamptz;
  txt  text;
  tot  bigint := 0;
  i    int;
BEGIN
  SELECT min(valid_from), max(valid_from) INTO lo, hi FROM doc_history;
  FOR i IN 1..n LOOP
    t := lo + random() * (hi - lo);
    SELECT doc::text INTO txt FROM doc_history
      WHERE valid_from <= t ORDER BY valid_from DESC LIMIT 1;
    tot := tot + coalesce(length(txt), 0);
  END LOOP;
  RETURN tot;
END;
$$ LANGUAGE plpgsql;

-- W3: list every version timestamp.
CREATE OR REPLACE FUNCTION bench_w3() RETURNS bigint AS $$
DECLARE cnt bigint := 0; r record;
BEGIN
  FOR r IN SELECT rev, valid_from FROM doc_history ORDER BY valid_from LOOP
    cnt := cnt + 1;
  END LOOP;
  RETURN cnt;
END;
$$ LANGUAGE plpgsql;

-- W4: one field's value across every version (returns the cross-check sum).
CREATE OR REPLACE FUNCTION bench_w4() RETURNS TABLE(n bigint, total numeric) AS $$
  SELECT count(c), sum(c) FROM (
    SELECT (doc->>'counter')::bigint AS c FROM doc_history ORDER BY valid_from
  ) s;
$$ LANGUAGE sql;

-- W6: no native diff — a representative TOP-LEVEL compare (not semantically equivalent to
-- SirixDB's node-level diff; see caveat #5).
CREATE OR REPLACE FUNCTION bench_w6(rev_a bigint, rev_b bigint)
RETURNS TABLE(key text, old_value jsonb, new_value jsonb) AS $$
  SELECT coalesce(a.key, b.key), a.value, b.value
  FROM (SELECT (jsonb_each(doc)).* FROM doc_history WHERE rev = rev_a) a
  FULL OUTER JOIN (SELECT (jsonb_each(doc)).* FROM doc_history WHERE rev = rev_b) b
    ON a.key = b.key
  WHERE a.value IS DISTINCT FROM b.value;
$$ LANGUAGE sql;
