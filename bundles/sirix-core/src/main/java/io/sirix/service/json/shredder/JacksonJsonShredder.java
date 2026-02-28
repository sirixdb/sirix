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
import io.sirix.access.trx.node.json.InternalJsonNodeTrx;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.ByteStringValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

/**
 * High-performance JSON shredder using Jackson's streaming parser.
 * 
 * <p>
 * This class is a drop-in replacement for {@link JsonShredder} that uses Jackson's streaming API
 * instead of Gson. Jackson provides better throughput while maintaining true streaming capability
 * for processing arbitrarily large JSON files with O(1) memory.
 * 
 * <p>
 * The implementation maintains full behavioral equivalence with {@link JsonShredder}, including
 * support for all insertion positions and the skipRootJson feature.
 * 
 * <h2>Thread Safety</h2>
 * <p>
 * Instances of this class are NOT thread-safe. Each thread should use its own instance. The
 * underlying Jackson parser is also not thread-safe.
 * 
 * <h2>Resource Management</h2>
 * <p>
 * The caller is responsible for closing the {@link JsonParser} after shredding. Use
 * try-with-resources or explicit close() calls.
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

  /** Internal interface for byte[] string insertion overloads. */
  private final InternalJsonNodeTrx internalWtx;

  /** Reusable value wrapper for pre-encoded UTF-8 string bytes (object record path). */
  private final ByteStringValue reusableByteStringValue = new ByteStringValue();

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

  /** First token — set by Builder to avoid PeekedTokenJsonParser wrapper. */
  private final JsonToken firstToken;

  /** Node key of the first inserted root node. */
  private long insertedRootNodeKey;

  /**
   * Creates a shared JsonFactory with lenient parsing enabled. JsonFactory is thread-safe and should
   * be reused.
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

    /** First token — pre-consumed during validation, replayed by shredder. */
    private JsonToken firstToken;

    /**
     * Constructor.
     *
     * @param wtx {@link JsonNodeTrx} implementation
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
     * Set the first token (pre-consumed during validation).
     *
     * @param token the first token to replay
     * @return this builder instance
     */
    public Builder firstToken(final JsonToken token) {
      this.firstToken = token;
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
    if (!(wtx instanceof InternalJsonNodeTrx internal)) {
      throw new IllegalStateException(
          "JacksonJsonShredder requires an InternalJsonNodeTrx implementation, got: " + wtx.getClass().getName());
    }
    internalWtx = internal;
    parser = builder.parser;
    insert = builder.insert;
    commit = builder.commit;
    skipRootJson = builder.skipRootJsonToken;
    firstToken = builder.firstToken;

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

  // ==================== Main Content Insertion (token-forwarding pattern) ====================

  /**
   * Sets the inserted root node key if not yet set.
   *
   * @param key the node key to record
   */
  private void markRootIfUnset(final long key) {
    if (insertedRootNodeKey == -1) {
      insertedRootNodeKey = key;
    }
  }

  /**
   * Insert new content using Jackson's native pull-parsing with token-forwarding.
   * Each process method reads the current token's value, advances the parser, and returns
   * the next JsonToken for dispatch — eliminating all lookahead/peek/cache overhead.
   *
   * @throws SirixException if something went wrong while inserting
   */
  private void insertNewContent() {
    try {
      level = 0;
      insertedRootNodeKey = -1;
      var token = (firstToken != null) ? firstToken : parser.nextToken();

      while (token != null) {
        token = switch (token) {
          case START_OBJECT -> processBeginObject();
          case FIELD_NAME -> processName();
          case END_OBJECT -> processEndObject();
          case START_ARRAY -> processBeginArray();
          case END_ARRAY -> processEndArray();
          case VALUE_STRING -> processString();
          case VALUE_TRUE, VALUE_FALSE -> processBoolean();
          case VALUE_NULL -> processNull();
          case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> processNumber();
          default -> parser.nextToken();
        };
      }

      if (insertedRootNodeKey != -1) {
        wtx.moveTo(insertedRootNodeKey);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  // ==================== Process Methods (token-forwarding) ====================

  private JsonToken processString() throws IOException {
    final var utf8 = encodeCurrentTextToUtf8();
    final var next = parser.nextToken();
    final var nextIsParent = (next == JsonToken.FIELD_NAME || next == JsonToken.END_OBJECT);
    final var key = insertStringValue(utf8, nextIsParent);
    markRootIfUnset(key);
    return next;
  }

  private JsonToken processNumber() throws IOException {
    final var number = JsonNumber.stringToNumber(parser.getText());
    final var next = parser.nextToken();
    final var nextIsParent = (next == JsonToken.FIELD_NAME || next == JsonToken.END_OBJECT);
    final var key = insertNumberValue(number, nextIsParent);
    markRootIfUnset(key);
    return next;
  }

  private JsonToken processBoolean() throws IOException {
    final var bool = parser.getBooleanValue();
    final var next = parser.nextToken();
    final var nextIsParent = (next == JsonToken.FIELD_NAME || next == JsonToken.END_OBJECT);
    final var key = insertBooleanValue(bool, nextIsParent);
    markRootIfUnset(key);
    return next;
  }

  private JsonToken processNull() throws IOException {
    final var next = parser.nextToken();
    final var nextIsParent = (next == JsonToken.FIELD_NAME || next == JsonToken.END_OBJECT);
    final var key = insertNullValue(nextIsParent);
    markRootIfUnset(key);
    return next;
  }

  private JsonToken processBeginObject() throws IOException {
    level++;
    if (!(level == 1 && skipRootJson)) {
      final var key = addObject();
      markRootIfUnset(key);
    }
    return parser.nextToken();
  }

  private JsonToken processBeginArray() throws IOException {
    level++;
    if (!(level == 1 && skipRootJson)) {
      final var key = insertArray();
      markRootIfUnset(key);
    }
    return parser.nextToken();
  }

  private JsonToken processEndObject() throws IOException {
    level--;
    final var next = parser.nextToken();
    processTrxMovement(next);
    if (level == 0) {
      return null;
    }
    return next;
  }

  private JsonToken processEndArray() throws IOException {
    level--;
    final var next = parser.nextToken();
    processTrxMovement(next);
    if (level == 0) {
      return null;
    }
    return next;
  }

  private JsonToken processName() throws IOException {
    final var name = parser.getText();
    final var valueToken = parser.nextToken();
    return addObjectRecord(name, valueToken);
  }

  @SuppressWarnings("ConstantConditions")
  private void processTrxMovement(final JsonToken nextToken) {
    if (!(level == 0 && skipRootJson)) {
      parents.popLong();
      wtx.moveTo(parents.peekLong(0));

      if (nextToken == JsonToken.FIELD_NAME || nextToken == JsonToken.END_OBJECT) {
        parents.popLong();
        wtx.moveTo(parents.peekLong(0));
      }
    }
  }

  // ==================== UTF-8 Encoding ====================

  /**
   * Encode the current parser text to UTF-8 bytes without constructing a {@link String}.
   *
   * <p>Uses Jackson's {@code getTextCharacters()} which returns the internal char buffer
   * without String allocation. For ASCII-only content (the common case in JSON), this
   * performs a direct 1:1 char-to-byte copy. Non-ASCII content falls back to the JDK's
   * {@link StandardCharsets#UTF_8} encoder for correctness with surrogates, BMP multi-byte,
   * and emoji.
   *
   * @return UTF-8 encoded byte array
   * @throws IOException if the parser cannot provide text
   */
  private byte[] encodeCurrentTextToUtf8() throws IOException {
    final var chars = parser.getTextCharacters();
    final var off = parser.getTextOffset();
    final var len = parser.getTextLength();

    // Fast path: check if all ASCII
    boolean allAscii = true;
    for (int i = off, end = off + len; i < end; i++) {
      if (chars[i] >= 0x80) {
        allAscii = false;
        break;
      }
    }

    if (allAscii) {
      final var bytes = new byte[len];
      for (int i = 0; i < len; i++) {
        bytes[i] = (byte) chars[off + i];
      }
      return bytes;
    }

    // Non-ASCII fallback: delegate to JDK for correctness (surrogates, BMP multi-byte, emoji)
    return new String(chars, off, len).getBytes(StandardCharsets.UTF_8);
  }

  // ==================== Insert Methods ====================

  private long insertStringValue(final byte[] utf8Value, final boolean nextTokenIsParent) {
    requireNonNull(utf8Value);
    final long key;

    switch (insert) {
      case AS_FIRST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = internalWtx.insertStringValueAsFirstChild(utf8Value).getNodeKey();
        } else {
          key = internalWtx.insertStringValueAsRightSibling(utf8Value).getNodeKey();
        }
      }
      case AS_LAST_CHILD -> {
        if (parents.peekLong(0) == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = internalWtx.insertStringValueAsLastChild(utf8Value).getNodeKey();
        } else {
          key = internalWtx.insertStringValueAsRightSibling(utf8Value).getNodeKey();
        }
      }
      case AS_LEFT_SIBLING -> key = internalWtx.insertStringValueAsLeftSibling(utf8Value).getNodeKey();
      case AS_RIGHT_SIBLING -> key = internalWtx.insertStringValueAsRightSibling(utf8Value).getNodeKey();
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

  private JsonToken addObjectRecord(final String name, final JsonToken valueToken) throws IOException {
    assert name != null;

    final ObjectRecordValue<?> value;
    var isContainerValue = false;

    switch (valueToken) {
      case START_OBJECT -> {
        level++;
        value = ObjectValue.INSTANCE;
        isContainerValue = true;
      }
      case START_ARRAY -> {
        level++;
        value = ArrayValue.INSTANCE;
        isContainerValue = true;
      }
      case VALUE_STRING -> {
        reusableByteStringValue.set(encodeCurrentTextToUtf8());
        value = reusableByteStringValue;
      }
      case VALUE_TRUE -> value = BooleanValue.of(true);
      case VALUE_FALSE -> value = BooleanValue.of(false);
      case VALUE_NULL -> value = NullValue.INSTANCE;
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT ->
          value = new NumberValue(JsonNumber.stringToNumber(parser.getText()));
      default -> throw new AssertionError("Unexpected value token: " + valueToken);
    }

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

    if (isContainerValue) {
      parents.popLong();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
      return parser.nextToken();
    } else {
      final var next = parser.nextToken();
      final var isNextTokenParent = (next == JsonToken.FIELD_NAME || next == JsonToken.END_OBJECT);
      adaptTrxPosAndStack(isNextTokenParent, key);
      return next;
    }
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
            new JacksonJsonShredder.Builder(wtx, jsonParser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
        shredder.call();
      }
    } catch (IOException e) {
      throw new SirixIOException(e);
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1_000_000 + " ms].");
  }
}
