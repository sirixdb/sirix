package org.sirix.xquery.json;

import com.google.common.base.Preconditions;
import com.google.gson.stream.JsonReader;
import org.brackit.xquery.jsonitem.AbstractJsonItemCollection;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.TemporalJsonCollection;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.json.shredder.JsonShredder;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.node.XmlDBCollection;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class JsonDBCollection extends AbstractJsonItemCollection<JsonDBItem>
    implements TemporalJsonCollection<JsonDBItem>, AutoCloseable {

  /**
   * Logger.
   */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(XmlDBCollection.class));

  /**
   * ID sequence.
   */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /**
   * Sirix database.
   */
  private final Database<JsonResourceManager> database;

  /**
   * Unique ID.
   */
  private final int id;

  private JsonDBStore jsonDbStore;

  /**
   * Constructor.
   *
   * @param name     collection name
   * @param database Sirix {@link Database} reference
   */
  public JsonDBCollection(final String name, final Database<JsonResourceManager> database) {
    super(Preconditions.checkNotNull(name));
    this.database = Preconditions.checkNotNull(database);
    id = ID_SEQUENCE.incrementAndGet();
  }

  /**
   * Constructor.
   *
   * @param name     collection name
   * @param database Sirix {@link Database} reference
   */
  public JsonDBCollection(final String name, final Database<JsonResourceManager> database,
      final JsonDBStore jsonDBStore) {
    super(Preconditions.checkNotNull(name));
    this.database = Preconditions.checkNotNull(database);
    id = ID_SEQUENCE.incrementAndGet();
    this.jsonDbStore = Preconditions.checkNotNull(jsonDBStore);
  }

  public JsonDBCollection setJsonDBStore(final JsonDBStore jsonDBStore) {
    this.jsonDbStore = jsonDBStore;
    return this;
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof JsonDBCollection)) {
      return false;
    }

    final JsonDBCollection coll = (JsonDBCollection) other;
    return database.equals(coll.database);
  }

  @Override
  public int hashCode() {
    return database.hashCode();
  }

  /**
   * Get the unique ID.
   *
   * @return unique ID
   */
  public int getID() {
    return id;
  }

  /**
   * Get the underlying Sirix {@link Database}.
   *
   * @return Sirix {@link Database}
   */
  public Database<JsonResourceManager> getDatabase() {
    return database;
  }

  @Override
  public JsonDBItem getDocument(Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  @Override
  public JsonDBItem getDocument(String name, Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  private JsonDBItem getDocumentInternal(final String resName, final Instant pointInTime) {
    final JsonResourceManager resource = database.openResourceManager(resName);

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

    return getItem(trx);
  }

  private JsonDBItem getDocumentInternal(final String resName, final int revision) {
    final JsonResourceManager resource = database.openResourceManager(resName);
    final int version = revision == -1 ? resource.getMostRecentRevisionNumber() : revision;

    final JsonNodeReadOnlyTrx rtx = resource.beginNodeReadOnlyTrx(version);

    return getItem(rtx);
  }

  @Override
  public void delete() {
    try {
      Databases.removeDatabase(database.getDatabaseConfig().getDatabaseFile());
    } catch (final SirixIOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void remove(final long documentID) {
    if (documentID >= 0) {
      final String resource = database.getResourceName((int) documentID);
      if (resource != null) {
        database.removeResource(resource);
      }
    }
  }

  @Override
  public JsonDBItem getDocument(final int revision) {
    final List<Path> resources = database.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      final JsonResourceManager manager = database.openResourceManager(resources.get(0).getFileName().toString());
      final int version = revision == -1 ? manager.getMostRecentRevisionNumber() : revision;
      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);

      return getItem(rtx);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private JsonDBItem getItem(final JsonNodeReadOnlyTrx rtx) {
    if (rtx.hasFirstChild()) {
      rtx.moveToFirstChild();
      if (rtx.isObject())
        return new JsonDBObject(rtx, this);
      else if (rtx.isArray())
        return new JsonDBArray(rtx, this);
    }

    return null;
  }

  public JsonDBItem add(final String resourceName, final JsonReader reader) {
    try {
      String resName = resourceName;
      for (final Path resource : database.listResources()) {
        final String existingResourceName = resource.getFileName().toString();
        if (existingResourceName.equals(resourceName)) {
          resName = existingResourceName + "1";
          break;
        }
      }
      database.createResource(ResourceConfiguration.newBuilder(resName).useDeweyIDs(true).build());
      final JsonResourceManager manager = database.openResourceManager(resName);
      try (final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader);
      }
      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx();
      rtx.moveToDocumentRoot();
      return getItem(rtx);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void close() {
    jsonDbStore.removeDatabase(database);
    database.close();
  }

  @Override
  public long getDocumentCount() {
    return database.listResources().size();
  }

  @Override
  public JsonDBItem getDocument() {
    return getDocument(-1);
  }

  @Override
  public JsonDBItem getDocument(final String name, final int revision) {
    return getDocumentInternal(name, revision);
  }

  @Override
  public JsonDBItem getDocument(final String name) {
    return getDocument(name, -1);
  }

  @Override
  public Stream<JsonDBItem> getDocuments() {
    final List<Path> resources = database.listResources();
    final List<JsonDBItem> documents = new ArrayList<>(resources.size());

    resources.forEach(resourcePath -> {
      try {
        final String resourceName = resourcePath.getFileName().toString();
        final JsonResourceManager resource = database.openResourceManager(resourceName);
        final JsonNodeReadOnlyTrx rtx = resource.beginNodeReadOnlyTrx();

        if (rtx.moveToFirstChild().hasMoved()) {
          if (rtx.isObject())
            documents.add(new JsonDBObject(rtx, this));
          else if (rtx.isArray())
            documents.add(new JsonDBArray(rtx, this));
        }
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    });

    return new ArrayStream<>(documents.toArray(new JsonDBItem[0]));
  }

  @Override
  public JsonDBItem add(final String json) {
    Preconditions.checkNotNull(json);

    return add(JsonShredder.createStringReader(json));
  }

  private JsonDBItem add(final JsonReader reader) {
    try {
      final String resourceName = "resource" + (database.listResources().size() + 1);
      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useDeweyIDs(true)
                                                   .useTextCompression(true)
                                                   .buildPathSummary(true)
                                                   .build());
      final JsonResourceManager manager = database.openResourceManager(resourceName);
      try (final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader);
      }

      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx();
      return getItem(rtx);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public JsonDBItem add(final Path file) {
    Preconditions.checkNotNull(file);

    return add(JsonShredder.createFileReader(file));
  }

}
