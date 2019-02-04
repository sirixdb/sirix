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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.xdm.ElementNode;
import org.sirix.service.ShredderCommit;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.settings.Fixed;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import com.google.gson.stream.JsonReader;

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
  protected final JsonNodeTrx mWtx;

  /** {@link JsonReader} implementation. */
  protected final JsonReader mReader;

  /** Determines if changes are going to be commit right after shredding. */
  private final ShredderCommit mCommit;

  /** Keeps track of visited keys. */
  private final Deque<Long> mParents;

  /** Insertion position. */
  private Insert mInsert;

  /**
   * Builder to build an {@link JsonShredder} instance.
   */
  public static class Builder {

    /** {@link JsonNodeTrx} implementation. */
    private final JsonNodeTrx mWtx;

    /** {@link JsonReader} implementation. */
    private final JsonReader mReader;

    /** Insertion position. */
    private final Insert mInsert;

    /**
     * Determines if after shredding the transaction should be immediately commited.
     */
    private ShredderCommit mCommit = ShredderCommit.NOCOMMIT;

    /**
     * Constructor.
     *
     * @param wtx {@link JsonNodeTrx} implementation
     * @param reader {@link JsonReader} implementation
     * @param insert insertion position
     * @throws NullPointerException if one of the arguments is {@code null}
     */
    public Builder(final JsonNodeTrx wtx, final JsonReader reader, final Insert insert) {
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
  protected final void insertNewContent() throws SirixException {
    try {
      int level = 0;
      boolean endElemReached = false;
      long insertedRootNodeKey = -1;

      // Iterate over all nodes.
      while (mReader.hasNext() && !endElemReached) {
        final var nextToken = mReader.peek();

        switch (nextToken) {
          case BEGIN_OBJECT:
            level++;
            mReader.beginObject();
            addObject();
            break;
          case NAME:
            final String name = mReader.nextName();
            addObjectKeyName(name);
            break;
          case END_OBJECT:
            level--;
            mReader.endObject();
            if (level == 0) {
              endElemReached = true;
            }
            mParents.pop();
            mWtx.moveTo(mParents.peek());
            break;
          case BEGIN_ARRAY:
            level++;
            mReader.beginArray();
            addArray();
            break;
          case END_ARRAY:
            level--;
            mReader.endArray();
            if (level == 0) {
              endElemReached = true;
            }
            mParents.pop();
            mWtx.moveTo(mParents.peek());
            break;
          case STRING:
            final var string = mReader.nextString();
            insertStringValue(string);
            break;
          case BOOLEAN:
            final var bool = mReader.nextBoolean();
            insertBooleanValue(bool);
            break;
          case NULL:
            mReader.nextNull();
            insertNullValue();
            break;
          case NUMBER:
            Number number;
            try {
              number = mReader.nextInt();
            } catch (final NumberFormatException e) {
              try {
                number = mReader.nextLong();
              } catch (final NumberFormatException nfe) {
                number = mReader.nextDouble();
              }
            }
            insertNumberValue(number);
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

  private void insertStringValue(final String stringValue) {
    final String value = checkNotNull(stringValue);
    if (!value.isEmpty()) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertStringValueAsFirstChild(value).getNodeKey();
      } else {
        key = mWtx.insertStringValueAsRightSibling(value).getNodeKey();
      }

      mParents.pop();
      mParents.push(key);
    }
  }

  private void insertBooleanValue(final boolean boolValue) {
    final boolean value = checkNotNull(boolValue);
    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertBooleanValueAsFirstChild(value).getNodeKey();
    } else {
      key = mWtx.insertBooleanValueAsRightSibling(value).getNodeKey();
    }

    mParents.pop();
    mParents.push(key);
  }

  private void insertNumberValue(final Number numberValue) {
    final Number value = checkNotNull(numberValue);

    if (value != null) {
      final long key;

      if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        key = mWtx.insertNumberValueAsFirstChild(value).getNodeKey();
      } else {
        key = mWtx.insertNumberValueAsRightSibling(value).getNodeKey();
      }

      mParents.pop();
      mParents.push(key);
    }
  }

  private void insertNullValue() {
    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertNullValueAsFirstChild().getNodeKey();
    } else {
      key = mWtx.insertNullValueAsRightSibling().getNodeKey();
    }

    mParents.pop();
    mParents.push(key);
  }

  private void addArray() {
    long key = -1;
    switch (mInsert) {
      case ASFIRSTCHILD:
        if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = mWtx.insertArrayAsFirstChild().getNodeKey();
        } else {
          key = mWtx.insertArrayAsRightSibling().getNodeKey();
        }
        break;
      case ASRIGHTSIBLING:
        if (mWtx.getKind() == Kind.JSON_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = mWtx.insertArrayAsRightSibling().getNodeKey();
        mInsert = Insert.ASFIRSTCHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    mParents.pop();
    mParents.push(key);
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  private void addObject() {
    long key = -1;
    switch (mInsert) {
      case ASFIRSTCHILD:
        if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          key = mWtx.insertObjectAsFirstChild().getNodeKey();
        } else {
          key = mWtx.insertObjectAsRightSibling().getNodeKey();
        }
        break;
      case ASRIGHTSIBLING:
        if (mWtx.getKind() == Kind.JSON_DOCUMENT
            || mWtx.getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          throw new IllegalStateException(
              "Subtree can not be inserted as sibling of document root or the root-object/array/whatever!");
        }
        key = mWtx.insertObjectAsRightSibling().getNodeKey();
        mInsert = Insert.ASFIRSTCHILD;
        break;
      // $CASES-OMITTED$
      default:
        throw new AssertionError();// Must not happen.
    }

    mParents.pop();
    mParents.push(key);
    mParents.push(Fixed.NULL_NODE_KEY.getStandardProperty());
  }

  /**
   * Add a new element node.
   *
   * @param name the key name
   * @return the modified stack
   * @throws SirixException if adding {@link ElementNode} fails
   */
  private void addObjectKeyName(final String name) {
    assert name != null;

    final long key;

    if (mParents.peek() == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      key = mWtx.insertObjectKeyAsFirstChild(name).getNodeKey();
    } else {
      key = mWtx.insertObjectKeyAsRightSibling(name).getNodeKey();
    }

    mParents.pop();
    mParents.push(key);
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
      db.createResource(new ResourceConfiguration.Builder("shredded", databaseConfig).build());
      try (final var resMgr = db.getResourceManager("shredded"); final var wtx = resMgr.beginNodeTrx()) {
        final var path = Paths.get(args[0]);
        final var jsonReader = createFileReader(path);
        final var shredder = new JsonShredder.Builder(wtx, jsonReader, Insert.ASFIRSTCHILD).commitAfterwards().build();
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
  public static synchronized JsonReader createFileReader(final Path path) {
    checkNotNull(path);

    try {
      final var fileReader = new FileReader(path.toFile());
      return new JsonReader(fileReader);
    } catch (final FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }
}
