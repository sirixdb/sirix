package org.sirix.xquery.json;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.brackit.xquery.jsonitem.AbstractJsonItemCollection;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.TemporalJsonCollection;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.Transaction;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.node.XmlDBCollection;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;

public final class JsonDBCollection extends AbstractJsonItemCollection<JsonDBNode>
    implements TemporalJsonCollection<JsonDBNode>, AutoCloseable {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(XmlDBCollection.class));

  /** ID sequence. */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /** {@link Sirix} database. */
  private final Database<JsonResourceManager> mDatabase;

  /** Unique ID. */
  private final int mID;

  /**
   * Constructor.
   *
   * @param name collection name
   * @param database Sirix {@link Database} reference
   */
  public JsonDBCollection(final String name, final Database<JsonResourceManager> database) {
    super(Preconditions.checkNotNull(name));
    mDatabase = Preconditions.checkNotNull(database);
    mID = ID_SEQUENCE.incrementAndGet();
  }

  public Transaction beginTransaction() {
    return mDatabase.beginTransaction();
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (this == other)
      return true;

    if (!(other instanceof JsonDBCollection))
      return false;

    final JsonDBCollection coll = (JsonDBCollection) other;
    return mDatabase.equals(coll.mDatabase);
  }

  @Override
  public int hashCode() {
    return mDatabase.hashCode();
  }

  /**
   * Get the unique ID.
   *
   * @return unique ID
   */
  public int getID() {
    return mID;
  }

  /**
   * Get the underlying Sirix {@link Database}.
   *
   * @return Sirix {@link Database}
   */
  public Database<JsonResourceManager> getDatabase() {
    return mDatabase;
  }

  @Override
  public JsonDBNode getDocument(Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  @Override
  public JsonDBNode getDocument(String name, Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  private JsonDBNode getDocumentInternal(final String resName, final Instant pointInTime) {
    final JsonResourceManager resource = mDatabase.openResourceManager(resName);

    JsonNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(pointInTime);

    if (trx.getRevisionTimestamp().isAfter(pointInTime)) {
      final int revision = trx.getRevisionNumber();

      if (revision > 1) {
        trx.close();

        trx = resource.beginNodeReadOnlyTrx(revision - 1);
      } else {
        trx.close();

        return null;
      }
    }

    return new JsonDBNode(trx, this);
  }

  private JsonDBNode getDocumentInternal(final String resName, final int revision) {
    final JsonResourceManager resource = mDatabase.openResourceManager(resName);
    final int version = revision == -1
        ? resource.getMostRecentRevisionNumber()
        : revision;

    final JsonNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(version);

    return new JsonDBNode(trx, this);
  }

  @Override
  public void delete() {
    try {
      Databases.removeDatabase(mDatabase.getDatabaseConfig().getFile());
    } catch (final SirixIOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void remove(final long documentID) {
    if (documentID >= 0) {
      final String resource = mDatabase.getResourceName((int) documentID);
      if (resource != null) {
        mDatabase.removeResource(resource);
      }
    }
  }

  @Override
  public JsonDBNode getDocument(final @Nonnegative int revision) {
    final List<Path> resources = mDatabase.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      final JsonResourceManager manager = mDatabase.openResourceManager(resources.get(0).getFileName().toString());
      final int version = revision == -1
          ? manager.getMostRecentRevisionNumber()
          : revision;
      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);
      return new JsonDBNode(rtx, this);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  public JsonDBNode add(final String resourceName, final JsonReader reader) {
    try {
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName).useDeweyIDs(true).build());
      final JsonResourceManager resource = mDatabase.openResourceManager(resourceName);
      final JsonNodeTrx wtx = resource.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(reader);
      wtx.moveToDocumentRoot();
      return new JsonDBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void close() throws SirixException {
    mDatabase.close();
  }

  @Override
  public long getDocumentCount() {
    return mDatabase.listResources().size();
  }

  @Override
  public JsonDBNode getDocument() {
    return getDocument(-1);
  }

  @Override
  public JsonDBNode getDocument(final String name, final int revision) {
    return getDocumentInternal(name, revision);
  }

  @Override
  public JsonDBNode getDocument(final String name) {
    return getDocument(name, -1);
  }

  @Override
  public Stream<JsonDBNode> getDocuments() {
    final List<Path> resources = mDatabase.listResources();
    final List<JsonDBNode> documents = new ArrayList<>(resources.size());

    resources.forEach(resourcePath -> {
      try {
        final String resourceName = resourcePath.getFileName().toString();
        final JsonResourceManager resource = mDatabase.openResourceManager(resourceName);
        final JsonNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx();
        documents.add(new JsonDBNode(trx, this));
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    });

    return new ArrayStream<>(documents.toArray(new JsonDBNode[documents.size()]));
  }

  @Override
  public JsonDBNode add(final Path file) {
    Preconditions.checkNotNull(file);

    try {
      final String resourceName =
          new StringBuilder(2).append("resource").append(mDatabase.listResources().size() + 1).toString();
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                    .useDeweyIDs(true)
                                                    .useTextCompression(true)
                                                    .buildPathSummary(true)
                                                    .build());
      final JsonResourceManager resource = mDatabase.openResourceManager(resourceName);
      final JsonNodeTrx wtx = resource.beginNodeTrx();

      wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(file));

      return new JsonDBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

}
