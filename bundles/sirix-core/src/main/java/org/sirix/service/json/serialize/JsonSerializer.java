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

package org.sirix.service.json.serialize;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT;
import static org.sirix.service.xml.serialize.XmlSerializerProperties.S_INDENT_SPACES;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.brackit.xquery.util.serialize.Serializer;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.service.AbstractSerializer;
import org.sirix.service.xml.serialize.XmlSerializerProperties;
import org.sirix.settings.Constants;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

/**
 * <h1>JsonSerializer</h1>
 *
 * <p>
 * Most efficient way to serialize a subtree into an OutputStream. The encoding always is UTF-8.
 * Note that the OutputStream internally is wrapped by a BufferedOutputStream. There is no need to
 * buffer it again outside of this class.
 * </p>
 */
public final class JsonSerializer extends AbstractSerializer<JsonNodeReadOnlyTrx, JsonNodeTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonSerializer.class));

  /** OutputStream to write to. */
  private final Appendable mOut;

  /** Indent output. */
  private final boolean mIndent;

  /** Number of spaces to indent. */
  private final int mIndentSpaces;

  /** Determines if serializing with initial indentation. */
  private final boolean mWithInitialIndent;

  private final boolean mEmitXQueryResultSequence;

  private final boolean mSerializeTimestamp;

  /**
   * Initialize XMLStreamReader implementation with transaction. The cursor points to the node the
   * XMLStreamReader starts to read.
   *
   * @param resourceMgr resource manager to read the resource
   * @param nodeKey start node key
   * @param builder builder of XML Serializer
   * @param revision revision to serialize
   * @param revsions further revisions to serialize
   */
  private JsonSerializer(final JsonResourceManager resourceMgr, final @Nonnegative long nodeKey, final Builder builder,
      final boolean initialIndent, final @Nonnegative int revision, final int... revsions) {
    super(resourceMgr, nodeKey, revision, revsions);
    mOut = builder.mStream;
    mIndent = builder.mIndent;
    mIndentSpaces = builder.mIndentSpaces;
    mWithInitialIndent = builder.mInitialIndent;
    mEmitXQueryResultSequence = builder.mEmitXQueryResultSequence;
    mSerializeTimestamp = builder.mSerializeTimestamp;
  }

  /**
   * Emit node.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  @Override
  protected void emitNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      switch (rtx.getKind()) {
        case JSON_DOCUMENT:
          break;
        case OBJECT:
          // Emit start element.
          indent();
          mOut.append("{");
          if (!rtx.hasFirstChild()) {
            mOut.append("}");
            if (rtx.hasRightSibling())
              mOut.append(",");
          }
          break;
        case ARRAY:
          mOut.append("[");
          if (!rtx.hasFirstChild()) {
            mOut.append("]");
            if (rtx.hasRightSibling())
              mOut.append(",");
          }
          break;
        case OBJECT_KEY:
          mOut.append("\"" + rtx.getName().stringValue() + "\":");
          break;
        case BOOLEAN_VALUE:
          mOut.append(Boolean.valueOf(rtx.getValue()).toString());
          printCommaIfNeeded(rtx);
          break;
        case NULL_VALUE:
          mOut.append("null");
          printCommaIfNeeded(rtx);
          break;
        case NUMBER_VALUE:
          mOut.append(rtx.getValue());
          printCommaIfNeeded(rtx);
          break;
        case STRING_VALUE:
          mOut.append("\"" + rtx.getValue() + "\"");
          printCommaIfNeeded(rtx);
          break;
        // $CASES-OMITTED$
        default:
          throw new IllegalStateException("Node kind not known!");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  private void printCommaIfNeeded(final JsonNodeReadOnlyTrx rtx) throws IOException {
    final boolean hasRightSibling = rtx.hasRightSibling();

    if (hasRightSibling)
      mOut.append(",");
  }

  /**
   * Emit end element.
   *
   * @param rtx Sirix {@link JsonNodeReadOnlyTrx}
   */
  @Override
  protected void emitEndNode(final JsonNodeReadOnlyTrx rtx) {
    try {
      indent();
      switch (rtx.getKind()) {
        case ARRAY:
          mOut.append("]");
          break;
        case OBJECT:
          mOut.append("}");
          if (rtx.hasRightSibling())
            mOut.append(",");
          break;
        case OBJECT_KEY:
          if (rtx.hasRightSibling())
            mOut.append(",");
          break;
        // $CASES-OMITTED$
        default:
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitStartDocument() {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (length > 1) {
        mOut.append("{");

        if (mIndent) {
          // mOut.append(CharsForSerializing.NEWLINE.getBytes());
          mStack.push(Constants.NULL_ID_LONG);
        }

        mOut.append("\"sirix\":");
        mOut.append("[");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitEndDocument() {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (length > 1) {
        if (mIndent) {
          mStack.pop();
        }
        indent();

        mOut.append("]");

        indent();

        mOut.append("}");
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionStartNode(final @Nonnull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mEmitXQueryResultSequence || length > 1) {
        indent();

        if (length > 1 || mEmitXQueryResultSequence) {
          mOut.append("{");
          mOut.append("\"revisionNumber\":");
          mOut.append(Integer.toString(rtx.getRevisionNumber()));
          mOut.append(",");

          if (mSerializeTimestamp) {
            mOut.append("\"revisionTimestamp\":");
            mOut.append("\"" + DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).format(rtx.getRevisionTimestamp())
                + "\"");
            mOut.append(",");
          }

          mOut.append("\"revision\":");
        }

        if (rtx.hasFirstChild())
          mStack.push(Constants.NULL_ID_LONG);

        // if (mIndent) {
        // mOut.append(CharsForSerializing.NEWLINE.getBytes());
        // }
      }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  @Override
  protected void emitRevisionEndNode(final @Nonnull JsonNodeReadOnlyTrx rtx) {
    try {
      final int length = (mRevisions.length == 1 && mRevisions[0] < 0)
          ? (int) mResMgr.getMostRecentRevisionNumber()
          : mRevisions.length;

      if (mEmitXQueryResultSequence || length > 1) {
        if (rtx.moveToDocumentRoot().trx().hasFirstChild())
          mStack.pop();
        indent();
        mOut.append("}");

        if (hasMoreRevisionsToSerialize(rtx))
          mOut.append(",");
      }

      // if (mIndent) {
      // mOut.append(CharsForSerializing.NEWLINE.getBytes());
      // }
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

  private boolean hasMoreRevisionsToSerialize(final JsonNodeReadOnlyTrx rtx) {
    return rtx.getRevisionNumber() < mRevisions[mRevisions.length - 1] || (mRevisions.length == 1 && mRevisions[0] == -1
        && rtx.getRevisionNumber() < rtx.getResourceManager().getMostRecentRevisionNumber());
  }

  /**
   * Indentation of output.
   *
   * @throws IOException if can't indent output
   */
  private void indent() throws IOException {
    if (mIndent) {
      final int indentSpaces = mWithInitialIndent
          ? (mStack.size() + 1) * mIndentSpaces
          : mStack.size() * mIndentSpaces;
      for (int i = 0; i < indentSpaces; i++) {
        mOut.append(" ");
      }
    }
  }

  /**
   * Main method.
   *
   * @param args args[0] specifies the input-TT file/folder; args[1] specifies the output XML file.
   * @throws Exception any exception
   */
  public static void main(final String... args) throws Exception {
    if (args.length < 2 || args.length > 3) {
      throw new IllegalArgumentException("Usage: XMLSerializer input-TT output.xml");
    }

    LOGWRAPPER.info("Serializing '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.nanoTime();
    final Path target = Paths.get(args[1]);
    SirixFiles.recursiveRemove(target);
    Files.createDirectories(target.getParent());
    Files.createFile(target);

    final Path databaseFile = Paths.get(args[0]);
    final DatabaseConfiguration config = new DatabaseConfiguration(databaseFile);
    Databases.createJsonDatabase(config);
    try (final var db = Databases.openJsonDatabase(databaseFile)) {
      db.createResource(new ResourceConfiguration.Builder("shredded").build());

      try (final JsonResourceManager resMgr = db.openResourceManager("shredded");
          final FileWriter outputStream = new FileWriter(target.toFile())) {
        final JsonSerializer serializer = JsonSerializer.newBuilder(resMgr, outputStream).build();
        serializer.call();
      }
    }

    LOGWRAPPER.info(" done [" + (System.nanoTime() - time) / 1_000_000 + "ms].");
  }

  /**
   * Constructor, setting the necessary stuff.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param stream {@link OutputStream} to write to
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceManager resMgr, final Writer stream, final int... revisions) {
    return new Builder(resMgr, stream, revisions);
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param nodeKey root node key of subtree to shredder
   * @param stream {@link OutputStream} to write to
   * @param properties {@link XmlSerializerProperties} to use
   * @param revisions revisions to serialize
   */
  public static Builder newBuilder(final JsonResourceManager resMgr, final @Nonnegative long nodeKey,
      final Writer stream, final JsonSerializerProperties properties, final int... revisions) {
    return new Builder(resMgr, nodeKey, stream, properties, revisions);
  }

  /**
   * JsonSerializerBuilder to setup the JsonSerializer.
   */
  public static final class Builder {
    /**
     * Intermediate boolean for indendation, not necessary.
     */
    private boolean mIndent;

    /**
     * Intermediate number of spaces to indent, not necessary.
     */
    private int mIndentSpaces = 2;

    /** Stream to pipe to. */
    private final Appendable mStream;

    /** Resource manager to use. */
    private final JsonResourceManager mResourceMgr;

    /** Further revisions to serialize. */
    private int[] mVersions;

    /** Revision to serialize. */
    private int mVersion;

    /** Node key of subtree to shredder. */
    private long mNodeKey;

    /** Determines if an initial indent is needed or not. */
    private boolean mInitialIndent;

    /** Determines if it's an XQuery result sequence. */
    private boolean mEmitXQueryResultSequence;

    /** Determines if a timestamp should be serialized or not. */
    private boolean mSerializeTimestamp;

    /**
     * Constructor, setting the necessary stuff.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param stream {@link OutputStream} to write to
     * @param revisions revisions to serialize
     */
    public Builder(final JsonResourceManager resourceMgr, final Appendable stream, final int... revisions) {
      mNodeKey = 0;
      mResourceMgr = checkNotNull(resourceMgr);
      mStream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        mVersion = mResourceMgr.getMostRecentRevisionNumber();
      } else {
        mVersion = revisions[0];
        mVersions = new int[revisions.length - 1];
        for (int i = 0; i < revisions.length - 1; i++) {
          mVersions[i] = revisions[i + 1];
        }
      }
    }

    /**
     * Constructor.
     *
     * @param resourceMgr Sirix {@link ResourceManager}
     * @param nodeKey root node key of subtree to shredder
     * @param stream {@link OutputStream} to write to
     * @param properties {@link XmlSerializerProperties} to use
     * @param revisions revisions to serialize
     */
    public Builder(final JsonResourceManager resourceMgr, final @Nonnegative long nodeKey, final Writer stream,
        final JsonSerializerProperties properties, final int... revisions) {
      checkArgument(nodeKey >= 0, "nodeKey must be >= 0!");
      mResourceMgr = checkNotNull(resourceMgr);
      mNodeKey = nodeKey;
      mStream = checkNotNull(stream);
      if (revisions == null || revisions.length == 0) {
        mVersion = mResourceMgr.getMostRecentRevisionNumber();
      } else {
        mVersion = revisions[0];
        mVersions = new int[revisions.length - 1];
        for (int i = 0; i < revisions.length - 1; i++) {
          mVersions[i] = revisions[i + 1];
        }
      }
      final ConcurrentMap<?, ?> map = checkNotNull(properties.getProps());
      mIndent = checkNotNull((Boolean) map.get(S_INDENT[0]));
      mIndentSpaces = checkNotNull((Integer) map.get(S_INDENT_SPACES[0]));
    }

    /**
     * Specify the start node key.
     *
     * @param nodeKey node key to start serialization from (the root of the subtree to serialize)
     * @return this XMLSerializerBuilder reference
     */
    public Builder startNodeKey(final long nodeKey) {
      mNodeKey = nodeKey;
      return this;
    }

    /**
     * Sets an initial indentation.
     *
     * @return this {@link Builder} instance
     */
    public Builder withInitialIndent() {
      mInitialIndent = true;
      return this;
    }

    /**
     * Sets if the serialization is used for XQuery result sets.
     *
     * @return this {@link Builder} instance
     */
    public Builder isXQueryResultSequence() {
      mEmitXQueryResultSequence = true;
      return this;
    }

    /**
     * Sets if the serialization of timestamps of the revision(s) is used or not.
     *
     * @return this {@link Builder} instance
     */
    public Builder serializeTimestamp(boolean serializeTimestamp) {
      mSerializeTimestamp = serializeTimestamp;
      return this;
    }

    /**
     * Pretty prints the output.
     *
     * @return this {@link Builder} instance
     */
    public Builder prettyPrint() {
      mIndent = true;
      return this;
    }

    /**
     * The versions to serialize.
     *
     * @param revisions the versions to serialize
     * @return this {@link Builder} instance
     */
    public Builder revisions(final int[] revisions) {
      checkNotNull(revisions);

      mVersion = revisions[0];

      mVersions = new int[revisions.length - 1];
      for (int i = 0; i < revisions.length - 1; i++) {
        mVersions[i] = revisions[i + 1];
      }

      return this;
    }

    /**
     * Building new {@link Serializer} instance.
     *
     * @return a new {@link Serializer} instance
     */
    public JsonSerializer build() {
      return new JsonSerializer(mResourceMgr, mNodeKey, this, mInitialIndent, mVersion, mVersions);
    }
  }
}
