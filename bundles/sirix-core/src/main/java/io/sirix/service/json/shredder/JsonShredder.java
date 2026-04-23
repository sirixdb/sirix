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
   * When {@code true}, primitive-valued object fields are emitted as a single
   * fused {@code OBJECT_NAMED_*} record instead of the legacy
   * {@code OBJECT_KEY + primitive-value} pair (task #62).
   */
  private final boolean fuseNamedPrimitives;

  /** iter#30 flip: fusion is the DEFAULT storage shape. Primitive-valued object fields are
   * emitted as a single {@code OBJECT_NAMED_*} record, cutting the bench DB ~4.5x on disk.
   * Legacy callers that need the old {@code OBJECT_KEY + primitive-value} pair (diff walkers,
   * a couple of internal round-trip tests) opt OUT via {@link Builder#fuseNamedPrimitives(boolean)}. */
  private static final boolean DEFAULT_FUSE_NAMED_PRIMITIVES = true;

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

    /** Opt-in fused OBJECT_NAMED_* emission for primitive-valued object fields (task #62). */
    private boolean fuseNamedPrimitives = DEFAULT_FUSE_NAMED_PRIMITIVES;

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
     * Enable fused OBJECT_NAMED_* emission. When on, any {@code "key": primitive}
     * pair becomes a single slotted-page record instead of the legacy OBJECT_KEY +
     * primitive-value child pair.
     *
     * <p>Since iter#30 fusion is the default; calling this method without an argument is
     * retained as a no-op convenience for call-sites that want to be explicit.
     */
    public Builder fuseNamedPrimitives() {
      this.fuseNamedPrimitives = true;
      return this;
    }

    /**
     * Explicit toggle of fused OBJECT_NAMED_* emission. Pass {@code false} to opt OUT
     * of fusion and stay on the legacy OBJECT_KEY + primitive-value-child shape — used
     * by tests / diff walkers / FMSE that rely on the two-event preorder.
     */
    public Builder fuseNamedPrimitives(final boolean on) {
      this.fuseNamedPrimitives = on;
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
    fuseNamedPrimitives = builder.fuseNamedPrimitives;

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

    // Detect primitive-value case so we can emit a single fused OBJECT_NAMED_* record instead of
    // the legacy OBJECT_KEY + primitive-value pair. Gated behind an opt-in flag — legacy shred
    // remains the default so ~4600 existing tests still pass unmodified.
    final boolean isFusedCandidate = fuseNamedPrimitives
        && isPrimitiveValueKind(value.getKind())
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

    parents.popLong();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (wtx.getKind() == NodeKind.OBJECT || wtx.getKind() == NodeKind.ARRAY) {
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

    // Replace the left-sibling anchor on the stack with the new fused key. In legacy, after
    // primitive-child insertion, adaptTrxPosAndStack pops the sentinel + moves to the parent
    // OBJECT_KEY. The stack's new top is OBJECT_KEY (the anchor for the next NAME). In fused,
    // the fused node plays the OBJECT_KEY role, so we pop the previous anchor + push the
    // fused key and move the cursor there.
    parents.popLong();
    parents.push(fusedKey);

    final boolean isNextTokenParentToken =
        reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT;

    if (isNextTokenParentToken) {
      // Next token is NAME/END_OBJECT: cursor needs to be on the anchor for right-sibling insert
      // (OBJECT_NAMED_* accepted by insertObjectRecordWithPrimitiveAsRightSibling). It already
      // is, but moveTo is idempotent-cheap and mirrors the legacy explicit move.
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
