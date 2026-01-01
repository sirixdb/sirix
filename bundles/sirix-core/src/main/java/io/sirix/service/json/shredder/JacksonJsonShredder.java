/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package io.sirix.service.json.shredder;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * High-performance JSON shredder using Jackson's streaming parser.
 * 
 * <p>This class is a drop-in replacement for {@link JsonShredder} that uses Jackson's
 * streaming API instead of Gson. Jackson provides better throughput while maintaining
 * true streaming capability for processing arbitrarily large JSON files with O(1) memory.
 * 
 * <p>The implementation maintains full behavioral equivalence with {@link JsonShredder},
 * including support for all insertion positions and the skipRootJson feature.
 * 
 * <h2>Thread Safety</h2>
 * <p>Instances of this class are NOT thread-safe. Each thread should use its own instance.
 * The underlying Jackson parser is also not thread-safe.
 * 
 * <h2>Resource Management</h2>
 * <p>The caller is responsible for closing the {@link JsonParser} after shredding.
 * Use try-with-resources or explicit close() calls.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * @see JsonShredder
 */
@SuppressWarnings({"DuplicatedCode", "ConstantConditions"})
public final class JacksonJsonShredder implements Callable<Long> {

  /** Shared JsonFactory instance (thread-safe, reusable). */
  private static final JsonFactory JSON_FACTORY = createJsonFactory();

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JacksonJsonShredder.class));

  /** {@link JsonNodeTrx}. */
  private final JsonNodeTrx wtx;

  /** Jackson {@link JsonParser} implementation. */
  private final JsonParser parser;

  /** Determines if changes are going to be committed right after shredding. */
  private final ShredderCommit commit;

  /** Keeps track of visited keys (ancestor node keys). */
  private final LongStack parents;

  /** Insertion position. */
  private InsertPosition insert;

  /** Current nesting level (depth in JSON structure). */
  private int level;

  /** Whether to skip the root JSON token. */
  private final boolean skipRootJson;

  /**
   * Lookahead token buffer - enables Gson-like peek() behavior.
   * When non-null, contains the next token that peek() will return.
   * When null, peek() will advance the parser.
   */
  private JsonToken lookaheadToken;

  /**
   * Cached values for the lookahead token (used when the token contains data).
   */
  private String lookaheadTextValue;
  private Boolean lookaheadBooleanValue;

  /**
   * Creates a shared JsonFactory with lenient parsing enabled.
   * JsonFactory is thread-safe and should be reused.
   */
  private static JsonFactory createJsonFactory() {
    JsonFactory factory = new JsonFactory();
    // Enable lenient parsing features (equivalent to Gson's setLenient(true))
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true);
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
    return factory;
  }

  /**
   * Builder to build a {@link JacksonJsonShredder} instance.
   */
  public static class Builder {

    /** {@link JsonNodeTrx} implementation. */
    private final JsonNodeTrx wtx;

    /** Jackson {@link JsonParser} implementation. */
    private final JsonParser parser;

    /** Insertion position. */
    private final InsertPosition insert;

    /** Determines if after shredding the transaction should be immediately committed. */
    private ShredderCommit commit = ShredderCommit.NOCOMMIT;

    /** Whether to skip the root JSON token. */
    private boolean skipRootJsonToken;

    /**
     * Constructor.
     *
     * @param wtx    {@link JsonNodeTrx} implementation
     * @param parser Jackson {@link JsonParser} implementation
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonParser parser, final InsertPosition insert) {
      this.wtx = requireNonNull(wtx, "wtx must not be null");
      this.parser = requireNonNull(parser, "parser must not be null");
      this.insert = requireNonNull(insert, "insert must not be null");
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

    /**
     * Skip the root JSON token (first object or array).
     *
     * @return this builder instance
     */
    @SuppressWarnings("UnusedReturnValue")
    public Builder skipRootJsonToken() {
      skipRootJsonToken = true;
      return this;
    }

    /**
     * Build an instance.
     *
     * @return {@link JacksonJsonShredder} instance
     */
    public JacksonJsonShredder build() {
      return new JacksonJsonShredder(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder reference
   */
  private JacksonJsonShredder(final Builder builder) {
    wtx = builder.wtx;
    parser = builder.parser;
    insert = builder.insert;
    commit = builder.commit;
    skipRootJson = builder.skipRootJsonToken;

    parents = new LongArrayList();
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Invoking the shredder.
   *
   * @return revision of file
   * @throws SirixException if any kind of sirix exception has occurred
   */
  @Override
  public Long call() {
    final long revision = wtx.getRevisionNumber();
    insertNewContent();
    commit.commit(wtx);
    return revision;
  }

  // ==================== Lookahead Buffer Methods (Gson-like peek/consume) ====================

  /**
   * Peek at the next token WITHOUT consuming it.
   * This emulates Gson's JsonReader.peek() behavior.
   * 
   * @return the next token, or null if end of input
   * @throws IOException if parsing fails
   */
  private JsonToken peek() throws IOException {
    if (lookaheadToken == null) {
      // No buffered token, advance the parser
      lookaheadToken = parser.nextToken();
      if (lookaheadToken != null) {
        // Cache the value if applicable
        cacheCurrentValue();
      }
    }
    return lookaheadToken;
  }

  /**
   * Consume the current (peeked) token and clear the lookahead buffer.
   * After calling this, the next peek() will advance the parser.
   */
  private void consume() {
    lookaheadToken = null;
    lookaheadTextValue = null;
    lookaheadBooleanValue = null;
  }

  /**
   * Cache the current value from the parser (when we peek and buffer a token).
   */
  private void cacheCurrentValue() throws IOException {
    switch (lookaheadToken) {
      case VALUE_STRING, FIELD_NAME -> lookaheadTextValue = parser.getText();
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
        lookaheadTextValue = parser.getText(); // Store raw text for number parsing
      }
      case VALUE_TRUE -> lookaheadBooleanValue = Boolean.TRUE;
      case VALUE_FALSE -> lookaheadBooleanValue = Boolean.FALSE;
      default -> {
        // No value to cache for structural tokens
      }
    }
  }

  /**
   * Get the string value of the current (peeked) token.
   * Must be called when peek() returned VALUE_STRING.
   */
  private String nextString() throws IOException {
    if (lookaheadToken == null) {
      throw new IllegalStateException("Must call peek() before nextString()");
    }
    String value = lookaheadTextValue;
    consume();
    return value;
  }

  /**
   * Get the boolean value of the current (peeked) token.
   * Must be called when peek() returned VALUE_TRUE or VALUE_FALSE.
   */
  private boolean nextBoolean() throws IOException {
    if (lookaheadToken == null) {
      throw new IllegalStateException("Must call peek() before nextBoolean()");
    }
    boolean value = lookaheadBooleanValue;
    consume();
    return value;
  }

  /**
   * Consume a null token.
   * Must be called when peek() returned VALUE_NULL.
   */
  private void nextNull() {
    if (lookaheadToken == null) {
      throw new IllegalStateException("Must call peek() before nextNull()");
    }
    consume();
  }

  /**
   * Get the number value of the current (peeked) token.
   * Must be called when peek() returned VALUE_NUMBER_INT or VALUE_NUMBER_FLOAT.
   */
  private Number nextNumber() throws IOException {
    if (lookaheadToken == null) {
      throw new IllegalStateException("Must call peek() before nextNumber()");
    }
    Number value = JsonNumber.stringToNumber(lookaheadTextValue);
    consume();
    return value;
  }

  /**
   * Get the field name of the current (peeked) token.
   * Must be called when peek() returned FIELD_NAME.
   */
  private String nextName() throws IOException {
    if (lookaheadToken == null) {
      throw new IllegalStateException("Must call peek() before nextName()");
    }
    String value = lookaheadTextValue;
    consume();
    return value;
  }

  /**
   * Begin an object - consume START_OBJECT token.
   */
  private void beginObject() {
    if (lookaheadToken != JsonToken.START_OBJECT) {
      throw new IllegalStateException("Expected START_OBJECT but got " + lookaheadToken);
    }
    consume();
  }

  /**
   * End an object - consume END_OBJECT token.
   */
  private void endObject() {
    if (lookaheadToken != JsonToken.END_OBJECT) {
      throw new IllegalStateException("Expected END_OBJECT but got " + lookaheadToken);
    }
    consume();
  }

  /**
   * Begin an array - consume START_ARRAY token.
   */
  private void beginArray() {
    if (lookaheadToken != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Expected START_ARRAY but got " + lookaheadToken);
    }
    consume();
  }

  /**
   * End an array - consume END_ARRAY token.
   */
  private void endArray() {
    if (lookaheadToken != JsonToken.END_ARRAY) {
      throw new IllegalStateException("Expected END_ARRAY but got " + lookaheadToken);
    }
    consume();
  }

  // ==================== Main Content Insertion (matches Gson's logic exactly) ====================

  /**
   * Insert new content based on the Jackson streaming parser.
   * This method matches the exact logic of Gson's JsonShredder.insertNewContent().
   *
   * @throws SirixException if something went wrong while inserting
   */
  private void insertNewContent() {
    try {
      level = 0;
      boolean endReached = false;
      long insertedRootNodeKey = -1;

      // Iterate over all tokens - peek() returns next token WITHOUT consuming
      // This matches: while (reader.peek() != JsonToken.END_DOCUMENT && !endReached)
      while (peek() != null && !endReached) {
        final var nextToken = peek();

        switch (nextToken) {
          case START_OBJECT -> insertedRootNodeKey = processBeginObject(insertedRootNodeKey);
          case FIELD_NAME -> processName();
          case END_OBJECT -> endReached = processEndObject();
          case START_ARRAY -> insertedRootNodeKey = processBeginArray(insertedRootNodeKey);
          case END_ARRAY -> endReached = processEndArray();
          case VALUE_STRING -> insertedRootNodeKey = processString(insertedRootNodeKey);
          case VALUE_TRUE, VALUE_FALSE -> insertedRootNodeKey = processBoolean(insertedRootNodeKey);
          case VALUE_NULL -> insertedRootNodeKey = processNull(insertedRootNodeKey);
          case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> insertedRootNodeKey = processNumber(insertedRootNodeKey);
          default -> consume(); // Skip unknown tokens
        }
      }

      wtx.moveTo(insertedRootNodeKey);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  // ==================== Process Methods (matching Gson's logic exactly) ====================

  private long processNumber(long insertedRootNodeKey) throws IOException {
    final var number = readNumber();
    final var insertedNumberValueNodeKey =
        insertNumberValue(number, peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedNumberValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processNull(long insertedRootNodeKey) throws IOException {
    nextNull();
    final var insertedNullValueNodeKey =
        insertNullValue(peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedNullValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processBoolean(long insertedRootNodeKey) throws IOException {
    final var bool = nextBoolean();
    final var insertedBooleanValueNodeKey =
        insertBooleanValue(bool, peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT);
    if (insertedRootNodeKey == -1)
      insertedRootNodeKey = insertedBooleanValueNodeKey;
    return insertedRootNodeKey;
  }

  private long processString(long insertedRootNodeKey) throws IOException {
    final var string = nextString();
    final var insertedStringValueNodeKey =
        insertStringValue(string, peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT);
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
    endArray();
    processTrxMovement();
    return endReached;
  }

  private long processBeginArray(long insertedRootNodeKey) throws IOException {
    level++;
    beginArray();
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
    endObject();
    processTrxMovement();
    return endReached;
  }

  private void processName() throws IOException {
    final String name = nextName();
    addObjectRecord(name);
  }

  private long processBeginObject(long insertedRootNodeKey) throws IOException {
    level++;
    beginObject();
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

      if (peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT) {
        parents.popLong();
        wtx.moveTo(parents.peekLong(0));
      }
    }
  }

  private Number readNumber() throws IOException {
    // Use nextNumber() which handles caching correctly
    return nextNumber();
  }

  // ==================== Insert Methods (identical to Gson version) ====================

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = requireNonNull(stringValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertStringValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertStringValueAsLeftSibling(value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);
    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsFirstChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertBooleanValueAsLastChild(boolValue).getNodeKey();
        } else {
          key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertBooleanValueAsLeftSibling(boolValue).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertBooleanValueAsRightSibling(boolValue).getNodeKey();
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);
    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = requireNonNull(numberValue);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsFirstChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNumberValueAsLastChild(value).getNodeKey();
        } else {
          key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertNumberValueAsLeftSibling(value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);
    return key;
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertNullValueAsLastChild().getNodeKey();
        } else {
          key = wtx.insertNullValueAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertNullValueAsLeftSibling().getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertNullValueAsRightSibling().getNodeKey();
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);
    return key;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    parents.popLong();

    if (nextTokenIsParent) {
      wtx.moveTo(parents.peekLong(0));
    } else {
      parents.push(key);
    }
  }

  private long insertArray() {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsLastChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      case AS_RIGHT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertArrayAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsLastChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsLeftSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      case AS_RIGHT_SIBLING -> {
        if (wtx.getKind() == NodeKind.JSON_DOCUMENT
            || wtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = wtx.insertObjectAsRightSibling().getNodeKey();
        insert = InsertPosition.AS_FIRST_CHILD;
      }
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    parents.popLong();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name) throws IOException {
    assert name != null;

    final ObjectRecordValue<?> value = getObjectRecordValue();
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectRecordAsLastChild(name, value).getNodeKey();
        } else {
          key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = wtx.insertObjectRecordAsLeftSibling(name, value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
      default -> throw new AssertionError("Unknown insert position: " + insert);
    }

    parents.popLong();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (wtx.getKind() == NodeKind.OBJECT || wtx.getKind() == NodeKind.ARRAY) {
      parents.popLong();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      // Peek to check what's next - matches reader.peek() in Gson version
      final boolean isNextTokenParentToken = peek() == JsonToken.FIELD_NAME || peek() == JsonToken.END_OBJECT;

      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }

  /**
   * Get the value for an object record by examining the next token.
   * Matches Gson's getObjectRecordValue() exactly.
   */
  public ObjectRecordValue<?> getObjectRecordValue() throws IOException {
    final var nextToken = peek();
    
    return switch (nextToken) {
      case START_OBJECT -> {
        level++;
        beginObject();
        yield new ObjectValue();
      }
      case START_ARRAY -> {
        level++;
        beginArray();
        yield new ArrayValue();
      }
      case VALUE_TRUE, VALUE_FALSE -> {
        final boolean booleanVal = nextBoolean();
        yield new BooleanValue(booleanVal);
      }
      case VALUE_STRING -> {
        final String stringVal = nextString();
        yield new StringValue(stringVal);
      }
      case VALUE_NULL -> {
        nextNull();
        yield new NullValue();
      }
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
        final Number numberVal = nextNumber();
        yield new NumberValue(numberVal);
      }
      default -> throw new AssertionError("Unexpected token for object record value: " + nextToken);
    };
  }

  // ==================== Factory Methods ====================

  /**
   * Create a new {@link JsonParser} instance for a file.
   *
   * @param path the path to the file
   * @return a Jackson {@link JsonParser} instance
   * @throws UncheckedIOException if the file cannot be read
   */
  public static JsonParser createFileParser(final Path path) {
    requireNonNull(path, "path must not be null");

    try {
      return JSON_FACTORY.createParser(path.toFile());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Create a new {@link JsonParser} instance for a String.
   *
   * @param json the JSON as a string
   * @return a Jackson {@link JsonParser} instance
   * @throws UncheckedIOException if parsing fails
   */
  public static JsonParser createStringParser(final String json) {
    requireNonNull(json, "json must not be null");

    try {
      return JSON_FACTORY.createParser(json);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Create a new {@link JsonParser} instance for an InputStream.
   *
   * @param inputStream the input stream containing JSON
   * @return a Jackson {@link JsonParser} instance
   * @throws UncheckedIOException if parsing fails
   */
  public static JsonParser createInputStreamParser(final InputStream inputStream) {
    requireNonNull(inputStream, "inputStream must not be null");

    try {
      return JSON_FACTORY.createParser(inputStream);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Create a new {@link JsonParser} instance for a Reader.
   *
   * @param reader the reader containing JSON
   * @return a Jackson {@link JsonParser} instance
   * @throws UncheckedIOException if parsing fails
   */
  public static JsonParser createReaderParser(final Reader reader) {
    requireNonNull(reader, "reader must not be null");

    try {
      return JSON_FACTORY.createParser(reader);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Get the shared JsonFactory for creating custom parsers.
   *
   * @return the shared JsonFactory instance
   */
  public static JsonFactory getJsonFactory() {
    return JSON_FACTORY;
  }

  // ==================== Main Method ====================

  /**
   * Main method for command-line usage.
   *
   * @param args input and output files
   * @throws SirixException if a Sirix error occurs
   */
  public static void main(final String... args) {
    if (args.length != 2 && args.length != 3) {
      throw new IllegalArgumentException("Usage: JacksonJsonShredder JSONFile Database");
    }
    LOGWRAPPER.info("Shredding '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final var targetDatabasePath = Paths.get(args[1]);
    final var databaseConfig = new DatabaseConfiguration(targetDatabasePath);
    Databases.removeDatabase(targetDatabasePath);
    Databases.createJsonDatabase(databaseConfig);

    try (final var db = Databases.openJsonDatabase(targetDatabasePath)) {
      db.createResource(ResourceConfiguration.newBuilder("shredded").build());
      try (final var resMgr = db.beginResourceSession("shredded");
           final var wtx = resMgr.beginNodeTrx();
           final var jsonParser = createFileParser(Paths.get(args[0]))) {
        final var shredder =
            new JacksonJsonShredder.Builder(wtx, jsonParser, InsertPosition.AS_FIRST_CHILD)
                .commitAfterwards()
                .build();
        shredder.call();
      }
    } catch (IOException e) {
      throw new SirixIOException(e);
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1_000_000 + " ms].");
  }
}
