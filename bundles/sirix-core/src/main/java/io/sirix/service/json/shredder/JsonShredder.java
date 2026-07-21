package io.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.ShredderCommit;
import io.sirix.service.json.JsonNumber;
import io.sirix.settings.Fixed;
import io.sirix.utils.LogWrapper;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongStack;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * This class appends a given {@link JsonReader} to a {@link JsonNodeTrx} . The content of the
 * stream is added as a subtree. Based on an enum which identifies the point of insertion, the
 * subtree is either added as first child or as right sibling.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
@SuppressWarnings({"DuplicatedCode", "ConstantConditions"})
public final class JsonShredder implements Callable<Long> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonShredder.class));

  /** {@link JsonNodeTrx}. */
  private final JsonNodeTrx wtx;

  /** {@link JsonReader} implementation. */
  private final JsonReader reader;

  /** Determines if changes are going to be commit right after shredding. */
  private final ShredderCommit commit;

  /** Keeps track of visited keys. */
  private final LongStack parents;

  /** Insertion position. */
  private InsertPosition insert;

  private int level;

  private final boolean skipRootJson;

  /**
   * Builder to build an {@link JsonShredder} instance.
   */
  public static class Builder {

    /** {@link JsonNodeTrx} implementation. */
    private final JsonNodeTrx wtx;

    /** {@link JsonReader} implementation. */
    private final JsonReader reader;

    /** Insertion position. */
    private final InsertPosition insert;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    private boolean skipRootJsonToken;

    /**
     * Constructor.
     *
     * @param wtx {@link JsonNodeTrx} implementation
     * @param reader {@link JsonReader} implementation
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonReader reader, final InsertPosition insert) {
      this.wtx = requireNonNull(wtx);
      this.reader = requireNonNull(reader);
      this.insert = requireNonNull(insert);
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public Builder commitAfterwards() {
      commit = ShredderCommit.COMMIT;
      return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public Builder skipRootJsonToken() {
      skipRootJsonToken = true;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link JsonShredder} instance
     */
    public JsonShredder build() {
      return new JsonShredder(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder reference
   */
  private JsonShredder(final Builder builder) {
    wtx = builder.wtx;
    reader = builder.reader;
    insert = builder.insert;
    commit = builder.commit;
    skipRootJson = builder.skipRootJsonToken;

    parents = new LongArrayList();
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Invoking the shredder.
   *
   * @throws SirixException if any kind of sirix exception which has occured
   * @return revision of file
   */
  @Override
  public Long call() {
    final long revision = wtx.getRevisionNumber();
    insertNewContent();
    commit.commit(wtx);
    return revision;
  }

  /**
   * Insert new content based on a StAX parser {@link XMLStreamReader}.
   *
   * @throws SirixException if something went wrong while inserting
   */
  private void insertNewContent() {
    // Document-order fast lane (docs/BULK_INGESTION.md): cursor-free append primitives
    // driven by an explicit frame stack. tryBeginBulkStreamInsert() verifies every
    // precondition (empty resource, no dewey IDs, no per-node indexes, bulk hashing mode);
    // any unsupported shape falls through to the classic cursor-based loop below.
    if (insert == InsertPosition.AS_FIRST_CHILD && !skipRootJson
        && wtx instanceof InternalJsonNodeTrx internalTrx && internalTrx.tryBeginBulkStreamInsert()) {
      insertNewContentBulk(internalTrx);
      return;
    }
    try {
      level = 0;
      boolean endReached = false;
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (reader.peek() != JsonToken.END_DOCUMENT && !endReached) {
        final var nextToken = reader.peek();

        switch (nextToken) {
          case BEGIN_OBJECT -> insertedRootNodeKey = processBeginObject(insertedRootNodeKey);
          case NAME -> processName();
          case END_OBJECT -> endReached = processEndObject();
          case BEGIN_ARRAY -> insertedRootNodeKey = processBeginArray(insertedRootNodeKey);
          case END_ARRAY -> endReached = processEndArray();
          case STRING -> insertedRootNodeKey = processString(insertedRootNodeKey);
          case BOOLEAN -> insertedRootNodeKey = processBoolean(insertedRootNodeKey);
          case NULL -> insertedRootNodeKey = processNull(insertedRootNodeKey);
          case NUMBER -> insertedRootNodeKey = processNumber(insertedRootNodeKey);
          default -> {
          }
          // Node kind not known.
        }
      }

      wtx.moveTo(insertedRootNodeKey);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Bulk-stream event loop: identical token consumption and node-emission order to the
   * classic loop, but every anchor comes from an explicit frame stack instead of the
   * transaction cursor. One frame per open container: parent node key, last emitted child
   * at that level ({@code NULL} before the first), and the path-summary context children
   * resolve against. Frames are parallel primitive arrays — zero allocation per node.
   */
  private void insertNewContentBulk(final InternalJsonNodeTrx bulkTrx) {
    try {
      long insertedRootNodeKey = -1;
      // Everything after a successful tryBeginBulkStreamInsert runs under the finally
      // that leaves bulk-stream mode — including this setup (interface contract).
      try {
        final long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
        long[] parentKeys = new long[64];
        long[] lastChildKeys = new long[64];
        long[] pathContexts = new long[64];
        int sp = 0;
        parentKeys[0] = wtx.getNodeKey(); // document root — guaranteed by the fast-lane gate
        lastChildKeys[0] = nullKey;
        pathContexts[0] = 0L;
        int depth = 0;
        boolean end = false;
        while (!end && reader.peek() != JsonToken.END_DOCUMENT) {
          switch (reader.peek()) {
            case BEGIN_OBJECT -> {
              reader.beginObject();
              depth++;
              final long key = bulkTrx.bulkInsertObject(parentKeys[sp], lastChildKeys[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
              if (++sp == parentKeys.length) {
                parentKeys = Arrays.copyOf(parentKeys, sp << 1);
                lastChildKeys = Arrays.copyOf(lastChildKeys, sp << 1);
                pathContexts = Arrays.copyOf(pathContexts, sp << 1);
              }
              parentKeys[sp] = key;
              lastChildKeys[sp] = nullKey;
              // Plain OBJECTs carry no path node — children resolve against the parent's context.
              pathContexts[sp] = pathContexts[sp - 1];
            }
            case BEGIN_ARRAY -> {
              reader.beginArray();
              depth++;
              final long key = bulkTrx.bulkInsertArray(parentKeys[sp], lastChildKeys[sp], pathContexts[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
              if (++sp == parentKeys.length) {
                parentKeys = Arrays.copyOf(parentKeys, sp << 1);
                lastChildKeys = Arrays.copyOf(lastChildKeys, sp << 1);
                pathContexts = Arrays.copyOf(pathContexts, sp << 1);
              }
              parentKeys[sp] = key;
              lastChildKeys[sp] = nullKey;
              pathContexts[sp] = bulkTrx.getLastBulkPathNodeKey();
            }
            case NAME -> {
              final String name = reader.nextName();
              switch (reader.peek()) {
                case BEGIN_OBJECT, BEGIN_ARRAY -> {
                  final NodeKind valueKind;
                  if (reader.peek() == JsonToken.BEGIN_OBJECT) {
                    reader.beginObject();
                    valueKind = NodeKind.OBJECT;
                  } else {
                    reader.beginArray();
                    valueKind = NodeKind.ARRAY;
                  }
                  depth++;
                  final long key = bulkTrx.bulkInsertObjectRecordStructural(name, valueKind,
                      parentKeys[sp], lastChildKeys[sp], pathContexts[sp]);
                  lastChildKeys[sp] = key;
                  if (++sp == parentKeys.length) {
                    parentKeys = Arrays.copyOf(parentKeys, sp << 1);
                    lastChildKeys = Arrays.copyOf(lastChildKeys, sp << 1);
                    pathContexts = Arrays.copyOf(pathContexts, sp << 1);
                  }
                  parentKeys[sp] = key;
                  lastChildKeys[sp] = nullKey;
                  pathContexts[sp] = bulkTrx.getLastBulkPathNodeKey();
                }
                case STRING -> lastChildKeys[sp] = bulkTrx.bulkInsertObjectRecordPrimitive(name,
                    new StringValue(reader.nextString()), parentKeys[sp], lastChildKeys[sp],
                    pathContexts[sp]);
                case NUMBER -> lastChildKeys[sp] = bulkTrx.bulkInsertObjectRecordPrimitive(name,
                    new NumberValue(readNumber()), parentKeys[sp], lastChildKeys[sp],
                    pathContexts[sp]);
                case BOOLEAN -> lastChildKeys[sp] = bulkTrx.bulkInsertObjectRecordPrimitive(name,
                    BooleanValue.of(reader.nextBoolean()), parentKeys[sp], lastChildKeys[sp],
                    pathContexts[sp]);
                case NULL -> {
                  reader.nextNull();
                  lastChildKeys[sp] = bulkTrx.bulkInsertObjectRecordPrimitive(name,
                      NullValue.INSTANCE, parentKeys[sp], lastChildKeys[sp], pathContexts[sp]);
                }
                default -> throw new AssertionError("Unexpected token after NAME: " + reader.peek());
              }
            }
            case END_OBJECT -> {
              reader.endObject();
              bulkTrx.bulkCloseContainer();
              sp--;
              if (--depth == 0) {
                end = true;
              }
            }
            case END_ARRAY -> {
              reader.endArray();
              bulkTrx.bulkCloseContainer();
              sp--;
              if (--depth == 0) {
                end = true;
              }
            }
            case STRING -> {
              final long key = bulkTrx.bulkInsertStringValue(reader.nextString(), parentKeys[sp],
                  lastChildKeys[sp], pathContexts[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
            }
            case NUMBER -> {
              final long key = bulkTrx.bulkInsertNumberValue(readNumber(), parentKeys[sp],
                  lastChildKeys[sp], pathContexts[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
            }
            case BOOLEAN -> {
              final long key = bulkTrx.bulkInsertBooleanValue(reader.nextBoolean(), parentKeys[sp],
                  lastChildKeys[sp], pathContexts[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
            }
            case NULL -> {
              reader.nextNull();
              final long key = bulkTrx.bulkInsertNullValue(parentKeys[sp], lastChildKeys[sp],
                  pathContexts[sp]);
              if (insertedRootNodeKey == -1) {
                insertedRootNodeKey = key;
              }
              lastChildKeys[sp] = key;
            }
            default -> {
            }
          }
        }
      } finally {
        bulkTrx.endBulkStreamInsert();
      }
      wtx.moveTo(insertedRootNodeKey);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private long processNumber(long insertedRootNodeKey) throws IOException {
    final var number = readNumber();
    final var insertedNumberValueNodeKey =
        insertNumberValue(number, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedNumberValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processNull(long insertedRootNodeKey) throws IOException {
    reader.nextNull();
    final var insertedNullValueNodeKey =
        insertNullValue(reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedNullValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processBoolean(long insertedRootNodeKey) throws IOException {
    final var bool = reader.nextBoolean();
    final var insertedBooleanValueNodeKey =
        insertBooleanValue(bool, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedBooleanValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processString(long insertedRootNodeKey) throws IOException {
    final var string = reader.nextString();
    final var insertedStringValueNodeKey =
        insertStringValue(string, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedStringValueNodeKey;
    return insertedRootNodeKey;
  }

  private boolean processEndArray() throws IOException {
    boolean endReached = false;
    level--;
    if (level == 0) {
      endReached = true;
    }
    reader.endArray();
    processTrxMovement();
    return endReached;
  }

  private long processBeginArray(long insertedRootNodeKey) throws IOException {
    level++;
    reader.beginArray();
    if (!(level == 1 && skipRootJson)) {
      final var insertedArrayNodeKey = insertArray();

      if (insertedRootNodeKey == -1)
        insertedRootNodeKey = insertedArrayNodeKey;
    }
    return insertedRootNodeKey;
  }

  private boolean processEndObject() throws IOException {
    boolean endReached = false;
    level--;
    if (level == 0) {
      endReached = true;
    }
    reader.endObject();
    processTrxMovement();
    return endReached;
  }

  private void processName() throws IOException {
    final String name = reader.nextName();
    addObjectRecord(name);
  }

  private long processBeginObject(long insertedRootNodeKey) throws IOException {
    level++;
    reader.beginObject();
    if (!(level == 1 && skipRootJson)) {
      final long insertedObjectNodeKey = addObject();

      if (insertedRootNodeKey == -1)
        insertedRootNodeKey = insertedObjectNodeKey;
    }

    return insertedRootNodeKey;
  }

  @SuppressWarnings("ConstantConditions")
  private void processTrxMovement() throws IOException {
    if (!(level == 0 && skipRootJson)) {
      parents.popLong();
      wtx.moveTo(parents.peekLong(0));

      if (reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT) {
        parents.popLong();
        wtx.moveTo(parents.peekLong(0));
      }
    }
  }

  private Number readNumber() throws IOException {
    final var stringValue = reader.nextString();

    return JsonNumber.stringToNumber(stringValue);
  }

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = requireNonNull(stringValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertStringValueAsLeftSibling(value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        break;
      default:
        throw new AssertionError();// Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsFirstChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsLastChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertBooleanValueAsLeftSibling(boolValue).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        break;
      default:
        throw new AssertionError();// Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = requireNonNull(numberValue);

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertNumberValueAsLeftSibling(value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        break;
      default:
        throw new AssertionError();// Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    parents.popLong();

    if (nextTokenIsParent)
      wtx.moveTo(parents.peekLong(0));
    else
      parents.push(key);
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsLastChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertNullValueAsLeftSibling().getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertNullValueAsRightSibling().getNodeKey();
        break;
      default:
        throw new AssertionError();// Should not happen
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertArray() {
    long key;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsLastChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_RIGHT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    long key;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsLastChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      case AS_RIGHT_SIBLING:
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name) throws IOException {
    assert name != null;

    final ObjectRecordValue<?> value = getObjectRecordValue();

    // Primitive-value object fields are always emitted as a single fused OBJECT_NAMED_* record
    // when the insertion position permits sibling insertion below the parent. Otherwise (left/right
    // sibling insertion of an object record), fall through to the generic OBJECT_KEY path so the
    // value subtree (object or array) gets its own OBJECT_KEY anchor.
    final boolean isFusedCandidate = isPrimitiveValueKind(value.getKind())
        && (insert == InsertPosition.AS_FIRST_CHILD || insert == InsertPosition.AS_LAST_CHILD);

    if (isFusedCandidate) {
      addObjectRecordFused(name, value);
      return;
    }

    final long key;

    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
        break;
      case AS_LAST_CHILD:
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsLastChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
        break;
      case AS_LEFT_SIBLING:
        key = wtx.insertObjectRecordAsLeftSibling(name, value).getNodeKey();
        break;
      case AS_RIGHT_SIBLING:
        key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        break;
      default:
        throw new AssertionError();// Should not happen
    }

    final NodeKind cursorKind = wtx.getKind();

    // P2: object/array-valued fields emit a single fused OBJECT_NAMED_OBJECT/ARRAY record. The
    // cursor lands ON the fused parent (NOT on a separate OBJECT/ARRAY child). The fused record
    // collapses the legacy OBJECT_KEY + OBJECT/ARRAY pair, but the parents stack arithmetic must
    // still match legacy expectations: processTrxMovement on END_OBJECT/END_ARRAY pops TWO levels
    // (one for the inner-field anchor, one for the OBJECT_KEY-equivalent). Mirror that by
    // pushing the fused key TWICE plus the NULL anchor — net +2 stack levels matching legacy's
    // [..., parent, key, NULL] → [..., fused, fused, NULL]. Both fused-key pops moveTo the same
    // (legitimate) cursor target, which is correct: under fusion the OBJECT_KEY level is
    // collapsed into the same record.
    if (cursorKind == NodeKind.OBJECT_NAMED_OBJECT || cursorKind == NodeKind.OBJECT_NAMED_ARRAY) {
      parents.popLong();
      parents.push(key);
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
      return;
    }

    parents.popLong();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (cursorKind == NodeKind.OBJECT || cursorKind == NodeKind.ARRAY) {
      parents.popLong();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      final boolean isNextTokenParentToken = reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT;

      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }

  /**
   * Emit a single fused OBJECT_NAMED_* record for a primitive-value object field.
   *
   * <p>After this returns, the transaction cursor is positioned on the fused node and the
   * parents stack mirrors the state produced by the legacy OBJECT_KEY-based shredder path
   * so downstream NAME / END_OBJECT handling stays unchanged — specifically, the fused node
   * takes the slot formerly occupied by OBJECT_KEY as "left sibling anchor" for the next
   * object field.
   */
  private void addObjectRecordFused(final String name, final ObjectRecordValue<?> value) throws IOException {
    final long fusedKey;
    if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      fusedKey = wtx.insertObjectRecordWithPrimitiveAsFirstChild(name, value).getNodeKey();
    } else {
      fusedKey = wtx.insertObjectRecordWithPrimitiveAsRightSibling(name, value).getNodeKey();
    }

    // Replace the left-sibling anchor on the stack with the new fused key. The fused node plays
    // the OBJECT_KEY role, so we pop the previous anchor + push the fused key and move the cursor
    // there. After cursor placement, downstream NAME / END_OBJECT handling stays unchanged.
    parents.popLong();
    parents.push(fusedKey);

    final boolean isNextTokenParentToken =
        reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT;

    if (isNextTokenParentToken) {
      // Next token is NAME/END_OBJECT: cursor needs to be on the anchor for right-sibling insert
      // (OBJECT_NAMED_* accepted by insertObjectRecordWithPrimitiveAsRightSibling). It already
      // is, but moveTo is idempotent-cheap.
      wtx.moveTo(fusedKey);
    }
  }

  private static boolean isPrimitiveValueKind(final NodeKind kind) {
    return kind == NodeKind.BOOLEAN_VALUE
        || kind == NodeKind.NUMBER_VALUE
        || kind == NodeKind.STRING_VALUE
        || kind == NodeKind.NULL_VALUE;
  }

  public ObjectRecordValue<?> getObjectRecordValue() throws IOException {
    final var nextToken = reader.peek();
    return switch (nextToken) {
      case BEGIN_OBJECT -> {
        level++;
        reader.beginObject();
        yield ObjectValue.INSTANCE;
      }
      case BEGIN_ARRAY -> {
        level++;
        reader.beginArray();
        yield ArrayValue.INSTANCE;
      }
      case BOOLEAN -> {
        final boolean booleanVal = reader.nextBoolean();
        yield BooleanValue.of(booleanVal);
      }
      case STRING -> {
        final String stringVal = reader.nextString();
        yield new StringValue(stringVal);
      }
      case NULL -> {
        reader.nextNull();
        yield NullValue.INSTANCE;
      }
      case NUMBER -> {
        final var numberVal = readNumber();
        yield new NumberValue(numberVal);
      }
      default -> throw new AssertionError();
    };
  }

  /**
   * Main method.
   *
   * @param args input and output files
   * @throws SirixException if a Sirix error occurs
   */
  public static void main(final String... args) {
    if (args.length != 2 && args.length != 3) {
      throw new IllegalArgumentException("Usage: JsonShredder JSONFile Database");
    }
    LOGWRAPPER.info("Shredding '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final var targetDatabasePath = Paths.get(args[1]);
    final var databaseConfig = new DatabaseConfiguration(targetDatabasePath);
    Databases.removeDatabase(targetDatabasePath);
    Databases.createJsonDatabase(databaseConfig);

    try (final var db = Databases.openJsonDatabase(targetDatabasePath)) {
      db.createResource(ResourceConfiguration.newBuilder("shredded").build());
      try (final var resMgr = db.beginResourceSession("shredded"); final var wtx = resMgr.beginNodeTrx()) {
        final var path = Paths.get(args[0]);
        final var jsonReader = createFileReader(path);
        final var shredder =
            new JsonShredder.Builder(wtx, jsonReader, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1000000 + " ms].");
  }

  /**
   * Create a new {@link JsonReader} instance on a file.
   *
   * @param path the path to the file
   * @return an {@link JsonReader} instance
   */
  public static JsonReader createFileReader(final Path path) {
    requireNonNull(path);

    try {
      final var fileReader = new FileReader(path.toFile());
      final var jsonReader = new JsonReader(fileReader);
      jsonReader.setLenient(true);
      return jsonReader;
    } catch (final FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Create a new {@link JsonReader} instance on a String.
   *
   * @param json the JSON as a string
   * @return an {@link JsonReader} instance
   */
  public static JsonReader createStringReader(final String json) {
    requireNonNull(json);

    final var stringReader = new StringReader(json);
    final var jsonReader = new JsonReader(stringReader);
    jsonReader.setLenient(true);
    return jsonReader;
  }
}
