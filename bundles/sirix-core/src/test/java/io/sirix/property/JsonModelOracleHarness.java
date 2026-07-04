package io.sirix.property;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.service.json.serialize.JsonSerializer;

import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Model-based oracle harness: applies the seed-determined operation sequence to a SirixDB JSON
 * resource AND to a trivially-correct in-memory model (plain {@link LinkedHashMap}/
 * {@link ArrayList} trees serialized by Jackson). After every commit the serialized resource must
 * be semantically identical to the model; at the end, every historical revision must still match
 * the snapshot taken right after its commit.
 *
 * <p>This catches semantic divergence that structural-invariant fuzzing cannot: an operation that
 * keeps the tree well-formed but lands in the wrong place, drops a sibling, or mutates the wrong
 * record still produces a well-formed — but wrong — document, and only an independent oracle
 * notices.
 *
 * <p>On failure the seed and full operation log are printed for exact reproduction. Shared by
 * {@link JsonModelBasedOracleTest} (fixed seeds, deterministic — safe for PIT's green-suite
 * requirement) and {@link JsonModelBasedOracleRandomTest} (fresh random seeds per run).
 */
final class JsonModelOracleHarness {

  private JsonModelOracleHarness() {
  }

  private static final ObjectMapper EXACT_MAPPER = JsonMapper.builder()
      .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
      .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
      .enable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS)
      .build();

  private static final ObjectMapper PLAIN_MAPPER = new ObjectMapper();

  private static final int OPS_PER_RUN = 60;
  private static final int OPS_PER_COMMIT = 5;

  static void runOracle(final long seed) {
    final Random random = new Random(seed);
    final List<String> opLog = new ArrayList<>();
    final List<Integer> revisions = new ArrayList<>();
    final List<String> snapshots = new ArrayList<>();

    try (final var database = JsonTestHelper.getDatabaseWithHashesEnabled(PATHS.PATH1.getFile());
         final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeTrx wtx = session.beginNodeTrx()) {

      // Root container: object or array, mirrored in the model.
      final Object modelRoot;
      if (random.nextBoolean()) {
        wtx.insertObjectAsFirstChild();
        modelRoot = new LinkedHashMap<String, Object>();
        opLog.add("root=object");
      } else {
        wtx.insertArrayAsFirstChild();
        modelRoot = new ArrayList<>();
        opLog.add("root=array");
      }
      wtx.commit();
      compareAgainstModel(session, modelRoot, seed, opLog);
      revisions.add(session.getMostRecentRevisionNumber());
      snapshots.add(serialize(session, session.getMostRecentRevisionNumber()));

      int sinceCommit = 0;
      for (int op = 0; op < OPS_PER_RUN; op++) {
        applyRandomOperation(wtx, modelRoot, random, opLog, op);
        if (++sinceCommit == OPS_PER_COMMIT) {
          sinceCommit = 0;
          wtx.commit();
          opLog.add("COMMIT -> r" + session.getMostRecentRevisionNumber());
          compareAgainstModel(session, modelRoot, seed, opLog);
          revisions.add(session.getMostRecentRevisionNumber());
          snapshots.add(serialize(session, session.getMostRecentRevisionNumber()));
        }
      }
      if (sinceCommit > 0) {
        wtx.commit();
        opLog.add("COMMIT -> r" + session.getMostRecentRevisionNumber());
        compareAgainstModel(session, modelRoot, seed, opLog);
        revisions.add(session.getMostRecentRevisionNumber());
        snapshots.add(serialize(session, session.getMostRecentRevisionNumber()));
      }

      // Temporal check: every historical revision still serializes exactly as it did
      // immediately after its commit.
      for (int i = 0; i < revisions.size(); i++) {
        assertEquals(snapshots.get(i), serialize(session, revisions.get(i)),
            "Revision " + revisions.get(i) + " changed after later commits [seed=" + seed + "]\nops=" + opLog);
      }
    } catch (final AssertionError e) {
      throw e;
    } catch (final Exception e) {
      fail("Exception [seed=" + seed + "]\nops=" + opLog, e);
    }
  }

  // ==================== OPERATION APPLICATION ====================

  private static void applyRandomOperation(final JsonNodeTrx wtx, final Object modelRoot, final Random random,
      final List<String> opLog, final int opNumber) {
    final List<Site> containers = new ArrayList<>();
    final List<Site> leaves = new ArrayList<>();
    collectSites(modelRoot, new ArrayList<>(), containers, leaves);

    // Try up to a few times to pick an applicable operation.
    for (int attempt = 0; attempt < 8; attempt++) {
      final int choice = random.nextInt(10);
      if (choice < 4) { // insert into a container (40%)
        final Site site = containers.get(random.nextInt(containers.size()));
        if (site.value() instanceof Map) {
          insertField(wtx, site, random, opLog, opNumber);
        } else {
          insertArrayElement(wtx, site, random, opLog, opNumber);
        }
        return;
      } else if (choice < 7) { // remove from a container (30%)
        final Site site = containers.get(random.nextInt(containers.size()));
        if (site.value() instanceof Map<?, ?> map && !map.isEmpty()) {
          removeField(wtx, site, random, opLog, opNumber);
          return;
        }
        if (site.value() instanceof List<?> list && !list.isEmpty()) {
          removeArrayElement(wtx, site, random, opLog, opNumber);
          return;
        }
      } else { // update a leaf value (30%)
        if (!leaves.isEmpty()) {
          updateLeaf(wtx, leaves.get(random.nextInt(leaves.size())), random, opLog, opNumber);
          return;
        }
      }
    }
    // Fall back to an insert at the root container, which is always applicable.
    final Site root = containers.get(0);
    if (root.value instanceof Map) {
      insertField(wtx, root, random, opLog, opNumber);
    } else {
      insertArrayElement(wtx, root, random, opLog, opNumber);
    }
  }

  @SuppressWarnings("unchecked")
  private static void insertField(final JsonNodeTrx wtx, final Site site, final Random random,
      final List<String> opLog, final int opNumber) {
    final Map<String, Object> object = (Map<String, Object>) site.value();
    final String key = freshKey(object, random);
    final Object value = randomValue(random);
    final boolean first = random.nextBoolean();

    navigateTo(wtx, site.path());
    if (first) {
      wtx.insertObjectRecordAsFirstChild(key, toRecordValue(value));
      final LinkedHashMap<String, Object> reordered = new LinkedHashMap<>();
      reordered.put(key, value);
      reordered.putAll(object);
      object.clear();
      object.putAll(reordered);
    } else {
      wtx.insertObjectRecordAsLastChild(key, toRecordValue(value));
      object.put(key, value);
    }
    opLog.add("op#" + opNumber + " insertField " + (first ? "first" : "last") + " at " + site.path()
        + " key=" + key + " value=" + describe(value));
  }

  @SuppressWarnings("unchecked")
  private static void insertArrayElement(final JsonNodeTrx wtx, final Site site, final Random random,
      final List<String> opLog, final int opNumber) {
    final List<Object> array = (List<Object>) site.value();
    final int index = array.isEmpty() ? 0 : random.nextInt(array.size() + 1);
    final Object value = randomValue(random);

    navigateTo(wtx, site.path());
    if (index == 0) {
      insertValueAsFirstChild(wtx, value);
    } else {
      moveToChild(wtx, index - 1);
      insertValueAsRightSibling(wtx, value);
    }
    array.add(index, value);
    opLog.add("op#" + opNumber + " insertArrayElement at " + site.path() + " index=" + index
        + " value=" + describe(value));
  }

  @SuppressWarnings("unchecked")
  private static void removeField(final JsonNodeTrx wtx, final Site site, final Random random,
      final List<String> opLog, final int opNumber) {
    final Map<String, Object> object = (Map<String, Object>) site.value();
    final List<String> keys = new ArrayList<>(object.keySet());
    final String key = keys.get(random.nextInt(keys.size()));

    navigateTo(wtx, site.path());
    moveToRecord(wtx, key);
    wtx.remove();
    object.remove(key);
    opLog.add("op#" + opNumber + " removeField at " + site.path() + " key=" + key);
  }

  @SuppressWarnings("unchecked")
  private static void removeArrayElement(final JsonNodeTrx wtx, final Site site, final Random random,
      final List<String> opLog, final int opNumber) {
    final List<Object> array = (List<Object>) site.value();
    final int index = random.nextInt(array.size());

    navigateTo(wtx, site.path());
    moveToChild(wtx, index);
    wtx.remove();
    array.remove(index);
    opLog.add("op#" + opNumber + " removeArrayElement at " + site.path() + " index=" + index);
  }

  private static void updateLeaf(final JsonNodeTrx wtx, final Site site, final Random random,
      final List<String> opLog, final int opNumber) {
    final Object oldValue = site.value();
    final Object newValue;
    if (oldValue instanceof String) {
      newValue = randomString(random);
      navigateTo(wtx, site.path());
      wtx.setStringValue((String) newValue);
    } else if (oldValue instanceof Boolean b) {
      newValue = !b;
      navigateTo(wtx, site.path());
      wtx.setBooleanValue((Boolean) newValue);
    } else if (oldValue instanceof Number) {
      newValue = randomNumber(random);
      navigateTo(wtx, site.path());
      wtx.setNumberValue((Number) newValue);
    } else {
      // null leaves have no in-place setter — replace via container ops instead; skip.
      opLog.add("op#" + opNumber + " skip update of null leaf at " + site.path());
      return;
    }
    replaceLeafInParent(site, newValue);
    opLog.add("op#" + opNumber + " updateLeaf at " + site.path() + " " + describe(oldValue)
        + " -> " + describe(newValue));
  }

  @SuppressWarnings("unchecked")
  private static void replaceLeafInParent(final Site site, final Object newValue) {
    final Object parent = site.parent();
    final Object lastSegment = site.path().get(site.path().size() - 1);
    if (parent instanceof Map) {
      ((Map<String, Object>) parent).put((String) lastSegment, newValue);
    } else {
      ((List<Object>) parent).set((Integer) lastSegment, newValue);
    }
  }

  // ==================== SIRIX NAVIGATION ====================

  /**
   * Positions the cursor on the node addressed by {@code path} (segments: String = object field,
   * Integer = array index). An empty path addresses the root container. Field segments land on the
   * fused OBJECT_NAMED_* record, which itself plays the container/value role.
   */
  private static void navigateTo(final JsonNodeTrx wtx, final List<Object> path) {
    wtx.moveToDocumentRoot();
    assertTrue(wtx.moveToFirstChild(), "document root must have a child");
    for (final Object segment : path) {
      if (segment instanceof String field) {
        moveToRecord(wtx, field);
      } else {
        moveToChild(wtx, (Integer) segment);
      }
    }
  }

  /** From an object container node, moves to the record (fused OBJECT_NAMED_*) named {@code key}. */
  private static void moveToRecord(final JsonNodeTrx wtx, final String key) {
    assertTrue(wtx.moveToFirstChild(), "object must have children when looking up key '" + key + "'");
    while (true) {
      final NodeKind kind = wtx.getKind();
      if (kind.playsObjectKeyRole() && key.equals(wtx.getName().getLocalName())) {
        return;
      }
      if (!wtx.moveToRightSibling()) {
        fail("record '" + key + "' not found in object");
      }
    }
  }

  /** From a container node, moves to its {@code index}-th child. */
  private static void moveToChild(final JsonNodeTrx wtx, final int index) {
    assertTrue(wtx.moveToFirstChild(), "container must have children");
    for (int i = 0; i < index; i++) {
      assertTrue(wtx.moveToRightSibling(), "container must have a child at index " + index);
    }
  }

  private static void insertValueAsFirstChild(final JsonNodeTrx wtx, final Object value) {
    if (value instanceof String s) {
      wtx.insertStringValueAsFirstChild(s);
    } else if (value instanceof Boolean b) {
      wtx.insertBooleanValueAsFirstChild(b);
    } else if (value instanceof Number n) {
      wtx.insertNumberValueAsFirstChild(n);
    } else if (value instanceof Map) {
      wtx.insertObjectAsFirstChild();
    } else if (value instanceof List) {
      wtx.insertArrayAsFirstChild();
    } else {
      wtx.insertNullValueAsFirstChild();
    }
  }

  private static void insertValueAsRightSibling(final JsonNodeTrx wtx, final Object value) {
    if (value instanceof String s) {
      wtx.insertStringValueAsRightSibling(s);
    } else if (value instanceof Boolean b) {
      wtx.insertBooleanValueAsRightSibling(b);
    } else if (value instanceof Number n) {
      wtx.insertNumberValueAsRightSibling(n);
    } else if (value instanceof Map) {
      wtx.insertObjectAsRightSibling();
    } else if (value instanceof List) {
      wtx.insertArrayAsRightSibling();
    } else {
      wtx.insertNullValueAsRightSibling();
    }
  }

  private static ObjectRecordValue<?> toRecordValue(final Object value) {
    if (value instanceof String s) {
      return new StringValue(s);
    }
    if (value instanceof Boolean b) {
      return new BooleanValue(b);
    }
    if (value instanceof Number n) {
      return new NumberValue(n);
    }
    if (value instanceof Map) {
      return new ObjectValue();
    }
    if (value instanceof List) {
      return new ArrayValue();
    }
    return new NullValue();
  }

  // ==================== MODEL WALKING / VALUE GENERATION ====================

  /** A location in the model: the value, its parent container, and the path from the root. */
  private record Site(Object value, Object parent, List<Object> path) {
  }

  @SuppressWarnings("unchecked")
  private static void collectSites(final Object node, final List<Object> path, final List<Site> containers,
      final List<Site> leaves) {
    collectSites(node, null, path, containers, leaves);
  }

  @SuppressWarnings("unchecked")
  private static void collectSites(final Object node, final Object parent, final List<Object> path,
      final List<Site> containers, final List<Site> leaves) {
    if (node instanceof Map<?, ?> map) {
      containers.add(new Site(node, parent, new ArrayList<>(path)));
      for (final Map.Entry<?, ?> entry : map.entrySet()) {
        path.add(entry.getKey());
        collectSites(entry.getValue(), node, path, containers, leaves);
        path.remove(path.size() - 1);
      }
    } else if (node instanceof List<?> list) {
      containers.add(new Site(node, parent, new ArrayList<>(path)));
      for (int i = 0; i < list.size(); i++) {
        path.add(i);
        collectSites(list.get(i), node, path, containers, leaves);
        path.remove(path.size() - 1);
      }
    } else {
      if (parent != null) {
        leaves.add(new Site(node, parent, new ArrayList<>(path)));
      }
    }
  }

  private static Object randomValue(final Random random) {
    return switch (random.nextInt(8)) {
      case 0, 1 -> randomString(random);
      case 2, 3 -> randomNumber(random);
      case 4 -> random.nextBoolean();
      case 5 -> null;
      case 6 -> new LinkedHashMap<String, Object>();
      default -> new ArrayList<>();
    };
  }

  private static final String[] EXOTIC_STRINGS = {"", "你好", "😀", "a\"b\\c", "line\nbreak", "tab\tchar"};

  private static String randomString(final Random random) {
    if (random.nextInt(5) == 0) {
      return EXOTIC_STRINGS[random.nextInt(EXOTIC_STRINGS.length)];
    }
    final int length = random.nextInt(10);
    final StringBuilder builder = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      builder.append((char) ('a' + random.nextInt(26)));
    }
    return builder.toString();
  }

  private static Number randomNumber(final Random random) {
    return switch (random.nextInt(4)) {
      case 0 -> random.nextInt();
      case 1 -> random.nextLong();
      case 2 -> random.nextInt(1000) + 0.25 * random.nextInt(4); // exact binary fractions
      default -> random.nextInt(100);
    };
  }

  private static String freshKey(final Map<String, Object> object, final Random random) {
    while (true) {
      final String key = "k" + random.nextInt(10_000);
      if (!object.containsKey(key)) {
        return key;
      }
    }
  }

  private static String describe(final Object value) {
    if (value instanceof Map) {
      return "{}";
    }
    if (value instanceof List) {
      return "[]";
    }
    if (value instanceof String s) {
      return "\"" + s + "\"";
    }
    return String.valueOf(value);
  }

  // ==================== ORACLE COMPARISON ====================

  private static void compareAgainstModel(final JsonResourceSession session, final Object modelRoot, final long seed,
      final List<String> opLog) throws Exception {
    final String modelJson = PLAIN_MAPPER.writeValueAsString(modelRoot);
    final String sirixJson = serialize(session, session.getMostRecentRevisionNumber());
    final JsonNode expected = EXACT_MAPPER.readTree(modelJson);
    final JsonNode actual = EXACT_MAPPER.readTree(sirixJson);
    assertEquals(expected, actual, () -> "Sirix diverged from the oracle model [seed=" + seed + "]\nmodel: "
        + modelJson + "\nsirix: " + sirixJson + "\nops=" + opLog);
  }

  private static String serialize(final JsonResourceSession session, final int revision) throws Exception {
    try (final Writer writer = new StringWriter()) {
      new JsonSerializer.Builder(session, writer, revision).build().call();
      return writer.toString();
    }
  }
}
