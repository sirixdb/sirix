/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.json.shredder;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.json.objectvalue.*;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.NodeKind;
import org.sirix.service.ShredderCommit;
import org.sirix.service.json.JsonNumber;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class appends a given {@link JsonReader} to a {@link JsonNodeTrx} . The content of the
 * stream is added as a subtree. Based on an enum which identifies the point of insertion, the
 * subtree is either added as first child or as right sibling.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
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
  private final Deque<Long> parents;

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
      this.wtx = checkNotNull(wtx);
      this.reader = checkNotNull(reader);
      this.insert = checkNotNull(insert);
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

    parents = new ArrayDeque<>();
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Invoking the shredder.
   *
   * @throws SirixException if any kind of sirix exception which has occured
   * @return revision of file
   */
  @Override
  public Long call() throws SirixException {
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
  protected final void insertNewContent() {
    try {
      level = 0;
      boolean endReached = false;
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (reader.peek() != JsonToken.END_DOCUMENT && !endReached) {
        final var nextToken = reader.peek();

        switch (nextToken) {
          case BEGIN_OBJECT:
            level++;
            reader.beginObject();
            if (!(level == 1 && skipRootJson)) {
              final long insertedObjectNodeKey = addObject();

              if (insertedRootNodeKey == -1)
                insertedRootNodeKey = insertedObjectNodeKey;
            }
            break;
          case NAME:
            final String name = reader.nextName();
            addObjectRecord(name);
            break;
          case END_OBJECT:
            level--;
            if (level == 0) {
              endReached = true;
            }

            reader.endObject();
            if (!(level == 0 && skipRootJson)) {
              parents.pop();
              wtx.moveTo(parents.peek());

              if (reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT) {
                parents.pop();
                wtx.moveTo(parents.peek());
              }
            }
            break;
          case BEGIN_ARRAY:
            level++;
            reader.beginArray();
            if (!(level == 1 && skipRootJson)) {
              final var insertedArrayNodeKey = insertArray();

              if (insertedRootNodeKey == -1)
                insertedRootNodeKey = insertedArrayNodeKey;
            }
            break;
          case END_ARRAY:
            level--;
            if (level == 0) {
              endReached = true;
            }

            reader.endArray();
            if (!(level == 0 && skipRootJson)) {
              parents.pop();
              wtx.moveTo(parents.peek());

              if (reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT) {
                parents.pop();
                wtx.moveTo(parents.peek());
              }
            }
            break;
          case STRING:
            final var string = reader.nextString();
            final var insertedStringValueNodeKey =
                insertStringValue(string, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedStringValueNodeKey;
            break;
          case BOOLEAN:
            final var bool = reader.nextBoolean();
            final var insertedBooleanValueNodeKey =
                insertBooleanValue(bool, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedBooleanValueNodeKey;
            break;
          case NULL:
            reader.nextNull();
            final var insertedNullValueNodeKey =
                insertNullValue(reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedNullValueNodeKey;
            break;
          case NUMBER:
            final var number = readNumber();

            final var insertedNumberValueNodeKey =
                insertNumberValue(number, reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedNumberValueNodeKey;
            break;
          case END_DOCUMENT:
          default:
            // Node kind not known.
        }
      }

      wtx.moveTo(insertedRootNodeKey);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private Number readNumber() throws IOException {
    final var stringValue = reader.nextString();

    return JsonNumber.stringToNumber(stringValue);
  }

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = checkNotNull(stringValue);
    final long key;

    if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = wtx.insertStringValueAsFirstChild(value).getNodeKey();
    } else {
      key = wtx.insertStringValueAsRightSibling(value).getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final boolean value = checkNotNull(boolValue);
    final long key;

    if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = wtx.insertBooleanValueAsFirstChild(value).getNodeKey();
    } else {
      key = wtx.insertBooleanValueAsRightSibling(value).getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = checkNotNull(numberValue);

    final long key;

    if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = wtx.insertNumberValueAsFirstChild(value).getNodeKey();
    } else {
      key = wtx.insertNumberValueAsRightSibling(value).getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    parents.pop();

    if (nextTokenIsParent)
      wtx.moveTo(parents.peek());
    else
      parents.push(key);
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = wtx.insertNullValueAsFirstChild().getNodeKey();
    } else {
      key = wtx.insertNullValueAsRightSibling().getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertArray() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertArrayAsRightSibling().getNodeKey();
        }
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

    parents.pop();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    long key = -1;
    switch (insert) {
      case AS_FIRST_CHILD:
        if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = wtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = wtx.insertObjectAsRightSibling().getNodeKey();
        }
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

    parents.pop();
    parents.push(key);
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name) throws IOException {
    assert name != null;

    final ObjectRecordValue<?> value = getObjectRecordValue();

    final long key;

    if (parents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = wtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
    } else {
      key = wtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
    }

    parents.pop();
    parents.push(wtx.getParentKey());
    parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (wtx.getKind() == NodeKind.OBJECT || wtx.getKind() == NodeKind.ARRAY) {
      parents.pop();
      parents.push(key);
      parents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      final boolean isNextTokenParentToken = reader.peek() == JsonToken.NAME || reader.peek() == JsonToken.END_OBJECT;

      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }

  public ObjectRecordValue<?> getObjectRecordValue() throws IOException {
    final var nextToken = reader.peek();
    final ObjectRecordValue<?> value;

    switch (nextToken) {
      case BEGIN_OBJECT:
        level++;
        reader.beginObject();

        value = new ObjectValue();

        break;
      case BEGIN_ARRAY:
        level++;
        reader.beginArray();

        value = new ArrayValue();

        break;
      case BOOLEAN:
        final boolean booleanVal = reader.nextBoolean();

        value = new BooleanValue(booleanVal);

        break;
      case STRING:
        final String stringVal = reader.nextString();

        value = new StringValue(stringVal);

        break;
      case NULL:
        reader.nextNull();
        value = new NullValue();
        break;
      case NUMBER:
        final var numberVal = readNumber();

        value = new NumberValue(numberVal);

        break;
      case END_ARRAY:
      case END_DOCUMENT:
      case END_OBJECT:
      case NAME:
      default:
        throw new AssertionError();
    }
    return value;
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
      try (final var resMgr = db.openResourceManager("shredded"); final var wtx = resMgr.beginNodeTrx()) {
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
    checkNotNull(path);

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
    checkNotNull(json);

    final var stringReader = new StringReader(json);
    final var jsonReader = new JsonReader(stringReader);
    jsonReader.setLenient(true);
    return jsonReader;
  }
}
