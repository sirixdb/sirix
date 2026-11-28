/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generated documents for the QRY tests: Lemire-shaped records
 * ({@code {"id","active","user":{"name","tags":[...]},"score","note"}}, the shape of the chapter's
 * background corpus) and the queries of the claimed shape over them.
 *
 * <p>
 * {@code jn:store} shreds sequentially, so node keys are document order: the document root is key
 * 0, the top-level array key 1, and records start at key 2. Every scalar field is one fused node, a
 * named object or array is one node, and each array element is one node, so a Lemire record with
 * {@code k} tags spans {@code 8 + k} keys in this order: object, {@code id}, {@code active},
 * {@code user}, {@code name}, {@code tags}, the {@code k} elements, {@code score}, {@code note}.
 * {@link #lemireNodes} and the offsets below are that arithmetic; tests that depend on it assert
 * the resulting node kinds with a read transaction before they trust it.
 */
public final class QryCorpus {

  /** The literal of the admin-sum query. */
  public static final String ADMIN = "admin";

  /** Slots per KeyValueLeafPage of the document index. */
  public static final int SLOTS = 1024;

  /** Key of the first record object in a {@code jn:store}d top-level array. */
  public static final long FIRST_RECORD_KEY = 2;

  /** Offsets inside a Lemire record whose tags array has {@code k} elements. */
  public static final int OFF_OBJECT = 0;
  public static final int OFF_ACTIVE = 2;
  public static final int OFF_USER = 3;
  public static final int OFF_TAGS = 5;
  public static final int OFF_FIRST_ELEMENT = 6;

  private static final String[] TAG_POOL = {"admin", "guest", "dev", "ops", "Admin", "admins", "dmin"};

  private QryCorpus() {
    throw new AssertionError("no instances");
  }

  /** Keys one Lemire record with {@code tags} elements spans. */
  public static int lemireNodes(final int tags) {
    return 8 + tags;
  }

  /** Offset of {@code score} in a Lemire record with {@code tags} elements. */
  public static int scoreOffset(final int tags) {
    return 6 + tags;
  }

  /** The admin-sum query of the chapter over {@code source} (QRY-504's query). */
  public static String adminSum(final String source) {
    return "sum(for $r in " + source + " where $r.active and $r.user.tags[] = \"admin\" return $r.score)";
  }

  /** The nested membership count of QRY-501. */
  public static String nestedMembershipCount(final String source) {
    return "count(for $r in " + source + " where $r.user.tags[] = \"admin\" return $r)";
  }

  /** The three aggregate forms of the chapter's scope, over one {@code where} clause. */
  public enum Form {
    /** {@code sum(... return $r.N)}. */
    SUM,
    /** {@code count(... return $r)}. */
    COUNT_RECORDS,
    /** {@code count(... return $r.N)}. */
    COUNT_FIELD;

    /** The query of this form over {@code source} with {@code where} and field {@code n}. */
    public String query(final String source, final String where, final String n) {
      return switch (this) {
        case SUM -> "sum(for $r in " + source + " where " + where + " return $r." + n + ")";
        case COUNT_RECORDS -> "count(for $r in " + source + " where " + where + " return $r)";
        case COUNT_FIELD -> "count(for $r in " + source + " where " + where + " return $r." + n + ")";
      };
    }
  }

  /**
   * One Lemire record as JSON.
   *
   * @param id the record's id
   * @param active the {@code active} value as JSON text, or {@code null} to omit the field
   * @param tags the {@code tags} elements as JSON texts
   * @param score the {@code score} value as JSON text, or {@code null} to omit the field
   */
  public static String lemire(final long id, final String active, final List<String> tags, final String score) {
    final StringBuilder sb = new StringBuilder(96 + tags.size() * 10);
    sb.append("{\"id\":").append(id);
    if (active != null) {
      sb.append(",\"active\":").append(active);
    }
    sb.append(",\"user\":{\"name\":\"u").append(id).append("\",\"tags\":[");
    for (int i = 0; i < tags.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(tags.get(i));
    }
    sb.append("]}");
    if (score != null) {
      sb.append(",\"score\":").append(score);
    }
    sb.append(",\"note\":\"n").append(id % 13).append("\"}");
    return sb.toString();
  }

  /** {@code s} as a JSON string literal (no escaping needed for the generated pools). */
  public static String str(final String s) {
    return '"' + s + '"';
  }

  /**
   * {@code n} random Lemire records with a fixed seed: {@code active} true on two in three, zero to
   * three tags from a pool holding {@value #ADMIN} and its near misses (case, prefix, suffix), and an
   * integral {@code score} in [-1000, 1000].
   */
  public static List<String> lemireRecords(final int n, final long seed) {
    return lemireRecords(n, seed, 0);
  }

  /** {@link #lemireRecords(int, long)} with ids starting at {@code firstId}. */
  public static List<String> lemireRecords(final int n, final long seed, final long firstId) {
    final Random rng = new Random(seed);
    final List<String> records = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final int k = rng.nextInt(4);
      final List<String> tags = new ArrayList<>(k);
      for (int t = 0; t < k; t++) {
        tags.add(str(TAG_POOL[rng.nextInt(TAG_POOL.length)]));
      }
      records.add(lemire(firstId + i, rng.nextInt(3) != 0
          ? "true"
          : "false", tags, Integer.toString(rng.nextInt(2001) - 1000)));
    }
    return records;
  }

  /** The records joined into one top-level JSON array. */
  public static String array(final List<String> records) {
    int length = 2;
    for (final String record : records) {
      length += record.length() + 1;
    }
    final StringBuilder sb = new StringBuilder(length);
    sb.append('[');
    for (int i = 0; i < records.size(); i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(records.get(i));
    }
    return sb.append(']').toString();
  }

  /**
   * Places a record so that the node at {@code offset} inside it lands on key {@code targetKey}.
   * Filler Lemire records (one non-matching tag, nine keys each) and one shim record whose tags pad
   * the remainder are appended to {@code records} first; the caller appends the target record next.
   *
   * @param records the records so far, starting at {@link #FIRST_RECORD_KEY}
   * @param nextKey the key the next appended record starts at
   * @param targetKey the key the node at {@code offset} of the next record must take
   * @param offset the node's offset inside the target record
   * @return the key the target record starts at
   */
  public static long placeNextRecord(final List<String> records, final long nextKey, final long targetKey,
      final int offset) {
    final long start = targetKey - offset;
    final int shimMinimum = lemireNodes(0);
    final long room = start - nextKey;
    if (room < 0 || (room > 0 && room < shimMinimum)) {
      throw new IllegalArgumentException("no room for a shim: next key " + nextKey + ", target start " + start);
    }
    long key = nextKey;
    long id = 1_000_000 + records.size();
    final int filler = lemireNodes(1);
    // f fillers leave a remainder in [shimMinimum, shimMinimum + filler), which one shim takes.
    final long fillers = room == 0
        ? 0
        : (room - shimMinimum) / filler;
    for (long f = 0; f < fillers; f++) {
      records.add(lemire(id++, "false", List.of(str("guest")), "1"));
      key += filler;
    }
    if (key != start) {
      final int pad = Math.toIntExact(start - key - shimMinimum);
      final List<String> tags = new ArrayList<>(pad);
      for (int i = 0; i < pad; i++) {
        tags.add(str("pad"));
      }
      records.add(lemire(id, "false", tags, "1"));
      key += lemireNodes(pad);
    }
    if (key != start) {
      throw new IllegalStateException("placement arithmetic: " + key + " != " + start);
    }
    return start;
  }
}
