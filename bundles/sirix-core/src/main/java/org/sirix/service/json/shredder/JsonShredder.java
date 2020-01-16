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

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.json.objectvalue.ArrayValue;
import org.sirix.access.trx.node.json.objectvalue.BooleanValue;
import org.sirix.access.trx.node.json.objectvalue.NullValue;
import org.sirix.access.trx.node.json.objectvalue.NumberValue;
import org.sirix.access.trx.node.json.objectvalue.ObjectRecordValue;
import org.sirix.access.trx.node.json.objectvalue.ObjectValue;
import org.sirix.access.trx.node.json.objectvalue.StringValue;
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
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

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
  private final JsonNodeTrx mWtx;

  /** {@link JsonReader} implementation. */
  private final JsonReader mReader;

  /** Determines if changes are going to be commit right after shredding. */
  private final ShredderCommit mCommit;

  /** Keeps track of visited keys. */
  private final Deque<Long> mParents;

  /** Insertion position. */
  private InsertPosition mInsert;

  private int mLevel;

  private final boolean mSkipRootJson;

  /**
   * Builder to build an {@link JsonShredder} instance.
   */
  public static class Builder {

    /** {@link JsonNodeTrx} implementation. */
    private final JsonNodeTrx mWtx;

    /** {@link JsonReader} implementation. */
    private final JsonReader mReader;

    /** Insertion position. */
    private final InsertPosition mInsert;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit mCommit = ShredderCommit.NOCOMMIT;

    private boolean mSkipRootJsonToken;

    /**
     * Constructor.
     *
     * @param wtx {@link JsonNodeTrx} implementation
     * @param reader {@link JsonReader} implementation
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonReader reader, final InsertPosition insert) {
      mWtx = checkNotNull(wtx);
      mReader = checkNotNull(reader);
      mInsert = checkNotNull(insert);
    }

    /**
     * Commit afterwards.
     *
     * @return this builder instance
     */
    public Builder commitAfterwards() {
      mCommit = ShredderCommit.COMMIT;
      return this;
    }

    public Builder skipRootJsonToken() {
      mSkipRootJsonToken = true;
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
    mWtx = builder.mWtx;
    mReader = builder.mReader;
    mInsert = builder.mInsert;
    mCommit = builder.mCommit;
    mSkipRootJson = builder.mSkipRootJsonToken;

    mParents = new ArrayDeque<>();
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Invoking the shredder.
   *
   * @throws SirixException if any kind of sirix exception which has occured
   * @return revision of file
   */
  @Override
  public Long call() throws SirixException {
    final long revision = mWtx.getRevisionNumber();
    insertNewContent();
    mCommit.commit(mWtx);
    return revision;
  }

  /**
   * Insert new content based on a StAX parser {@link XMLStreamReader}.
   *
   * @throws SirixException if something went wrong while inserting
   */
  protected final void insertNewContent() {
    try {
      mLevel = 0;
      boolean endReached = false;
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (mReader.peek() != JsonToken.END_DOCUMENT && !endReached) {
        final var nextToken = mReader.peek();

        switch (nextToken) {
          case BEGIN_OBJECT:
            mLevel++;
            mReader.beginObject();
            if (!(mLevel == 1 && mSkipRootJson)) {
              final long insertedObjectNodeKey = addObject();

              if (insertedRootNodeKey == -1)
                insertedRootNodeKey = insertedObjectNodeKey;
            }
            break;
          case NAME:
            final String name = mReader.nextName();
            addObjectRecord(name);
            break;
          case END_OBJECT:
            mLevel--;
            if (mLevel == 0) {
              endReached = true;
            }

            mReader.endObject();
            if (!(mLevel == 0 && mSkipRootJson)) {
              mParents.pop();
              mWtx.moveTo(mParents.peek());

              if (mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT) {
                mParents.pop();
                mWtx.moveTo(mParents.peek());
              }
            }
            break;
          case BEGIN_ARRAY:
            mLevel++;
            mReader.beginArray();
            if (!(mLevel == 1 && mSkipRootJson)) {
              final var insertedArrayNodeKey = insertArray();

              if (insertedRootNodeKey == -1)
                insertedRootNodeKey = insertedArrayNodeKey;
            }
            break;
          case END_ARRAY:
            mLevel--;
            if (mLevel == 0) {
              endReached = true;
            }

            mReader.endArray();
            if (!(mLevel == 0 && mSkipRootJson)) {
              mParents.pop();
              mWtx.moveTo(mParents.peek());

              if (mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT) {
                mParents.pop();
                mWtx.moveTo(mParents.peek());
              }
            }
            break;
          case STRING:
            final var string = mReader.nextString();
            final var insertedStringValueNodeKey =
                insertStringValue(string, mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedStringValueNodeKey;
            break;
          case BOOLEAN:
            final var bool = mReader.nextBoolean();
            final var insertedBooleanValueNodeKey =
                insertBooleanValue(bool, mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedBooleanValueNodeKey;
            break;
          case NULL:
            mReader.nextNull();
            final var insertedNullValueNodeKey =
                insertNullValue(mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedNullValueNodeKey;
            break;
          case NUMBER:
            final var number = readNumber();

            final var insertedNumberValueNodeKey =
                insertNumberValue(number, mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT);

            if (insertedRootNodeKey == -1)
              insertedRootNodeKey = insertedNumberValueNodeKey;
            break;
          case END_DOCUMENT:
          default:
            // Node kind not known.
        }
      }

      mWtx.moveTo(insertedRootNodeKey);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private Number readNumber() throws IOException {
    final var stringValue = mReader.nextString();

    return JsonNumber.stringToNumber(stringValue);
  }

  private long insertStringValue(final String stringValue, final boolean nextTokenIsParent) {
    final String value = checkNotNull(stringValue);
    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertStringValueAsFirstChild(value).getNodeKey();
    } else {
      key = mWtx.insertStringValueAsRightSibling(value).getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertBooleanValue(final boolean boolValue, final boolean nextTokenIsParent) {
    final boolean value = checkNotNull(boolValue);
    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertBooleanValueAsFirstChild(value).getNodeKey();
    } else {
      key = mWtx.insertBooleanValueAsRightSibling(value).getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertNumberValue(final Number numberValue, final boolean nextTokenIsParent) {
    final Number value = checkNotNull(numberValue);

    if (value != null) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertNumberValueAsFirstChild(value).getNodeKey();
      } else {
        key = mWtx.insertNumberValueAsRightSibling(value).getNodeKey();
      }

      adaptTrxPosAndStack(nextTokenIsParent, key);

      return key;
    }

    return -1;
  }

  private void adaptTrxPosAndStack(final boolean nextTokenIsParent, final long key) {
    mParents.pop();

    if (nextTokenIsParent)
      mWtx.moveTo(mParents.peek());
    else
      mParents.push(key);
  }

  private long insertNullValue(final boolean nextTokenIsParent) {
    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertNullValueAsFirstChild().getNodeKey();
    } else {
      key = mWtx.insertNullValueAsRightSibling().getNodeKey();
    }

    adaptTrxPosAndStack(nextTokenIsParent, key);

    return key;
  }

  private long insertArray() {
    long key = -1;
    switch (mInsert) {
      case AS_FIRST_CHILD:
        if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = mWtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = mWtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case AS_RIGHT_SIBLING:
        if (mWtx.getKind() == NodeKind.JSON_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = mWtx.insertArrayAsRightSibling().getNodeKey();
        mInsert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    mParents.pop();
    mParents.push(key);
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private long addObject() {
    long key = -1;
    switch (mInsert) {
      case AS_FIRST_CHILD:
        if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = mWtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = mWtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case AS_RIGHT_SIBLING:
        if (mWtx.getKind() == NodeKind.JSON_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = mWtx.insertObjectAsRightSibling().getNodeKey();
        mInsert = InsertPosition.AS_FIRST_CHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    mParents.pop();
    mParents.push(key);
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    return key;
  }

  private void addObjectRecord(final String name) throws IOException {
    assert name != null;

    final var nextToken = mReader.peek();
    final ObjectRecordValue<?> value;

    switch (nextToken) {
      case BEGIN_OBJECT:
        mLevel++;
        mReader.beginObject();

        value = new ObjectValue();

        break;
      case BEGIN_ARRAY:
        mLevel++;
        mReader.beginArray();

        value = new ArrayValue();

        break;
      case BOOLEAN:
        final boolean booleanVal = mReader.nextBoolean();

        value = new BooleanValue(booleanVal);

        break;
      case STRING:
        final String stringVal = mReader.nextString();

        value = new StringValue(stringVal);

        break;
      case NULL:
        mReader.nextNull();
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

    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertObjectRecordAsFirstChild(name, value).getNodeKey();
    } else {
      key = mWtx.insertObjectRecordAsRightSibling(name, value).getNodeKey();
    }

    mParents.pop();
    mParents.push(mWtx.getParentKey());
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());

    if (mWtx.getKind() == NodeKind.OBJECT || mWtx.getKind() == NodeKind.ARRAY) {
      mParents.pop();
      mParents.push(key);
      mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
    } else {
      final boolean isNextTokenParentToken = mReader.peek() == JsonToken.NAME || mReader.peek() == JsonToken.END_OBJECT;

      adaptTrxPosAndStack(isNextTokenParentToken, key);
    }
  }

  /**
   * Main method.
   *
   * @param args input and output files
   * @throws XMLStreamException if the XML stream isn't valid
   * @throws IOException if an I/O error occurs
   * @throws SirixException if a Sirix error occurs
   */
  public static void main(final String... args) throws SirixException, IOException, XMLStreamException {
    if (args.length != 2 && args.length != 3) {
      throw new IllegalArgumentException("Usage: XMLShredder XMLFile Database [true/false] (shredder comment|PI)");
    }
    LOGWRAPPER.info("Shredding '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final var targetDatabasePath = Paths.get(args[1]);
    final var databaseConfig = new DatabaseConfiguration(targetDatabasePath);
    Databases.removeDatabase(targetDatabasePath);
    Databases.createJsonDatabase(databaseConfig);

    try (final var db = Databases.openJsonDatabase(targetDatabasePath)) {
      db.createResource(new ResourceConfiguration.Builder("shredded").build());
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
      return new JsonReader(fileReader);
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
    return new JsonReader(stringReader);
  }
}
