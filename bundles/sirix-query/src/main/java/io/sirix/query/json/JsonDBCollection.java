package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.json.TemporalJsonCollection;
import io.brackit.query.jsonitem.AbstractJsonItemCollection;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.node.stream.ArrayStream;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.query.function.DateTimeToInstant;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

public final class JsonDBCollection extends AbstractJsonItemCollection<JsonDBItem>
    implements TemporalJsonCollection<JsonDBItem>, AutoCloseable {

  /**
   * Logger.
   */
  private static final LogWrapper LOG_WRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonDBCollection.class));

  /**
   * ID sequence.
   */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  private final DateTimeToInstant dateTimeToInstant = new DateTimeToInstant();

  /**
   * Sirix database.
   */
  private final Database<JsonResourceSession> database;

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
  public JsonDBCollection(final String name, final Database<JsonResourceSession> database) {
    super(requireNonNull(name));
    this.database = requireNonNull(database);
    id = ID_SEQUENCE.incrementAndGet();
  }

  /**
   * Constructor.
   *
   * @param name     collection name
   * @param database Sirix {@link Database} reference
   */
  public JsonDBCollection(final String name, final Database<JsonResourceSession> database,
      final JsonDBStore jsonDBStore) {
    super(requireNonNull(name));
    this.database = requireNonNull(database);
    id = ID_SEQUENCE.incrementAndGet();
    this.jsonDbStore = requireNonNull(jsonDBStore);
  }

  public JsonDBCollection setJsonDBStore(final JsonDBStore jsonDBStore) {
    this.jsonDbStore = jsonDBStore;
    return this;
  }

  @Override
  public boolean equals(final java.lang.Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof JsonDBCollection coll)) {
      return false;
    }

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
  public Database<JsonResourceSession> getDatabase() {
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
    final JsonResourceSession resource = database.beginResourceSession(resName);

    JsonNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(pointInTime);

    if (trx.getRevisionTimestamp().isAfter(pointInTime)) {
      final int revision = trx.getRevisionNumber();

      if (revision > 1) {
        trx.close();

        trx = resource.beginNodeReadOnlyTrx(revision - 1);
      } else if (revision == 0) {
        trx.close();

        trx = resource.beginNodeReadOnlyTrx(1);
      }
    }

    return getItem(trx);
  }

  private JsonDBItem getDocumentInternal(final String resName, final int revision) {
    final JsonResourceSession resource = database.beginResourceSession(resName);
    final int version = revision == -1 ? resource.getMostRecentRevisionNumber() : revision;

    final JsonNodeReadOnlyTrx rtx = resource.beginNodeReadOnlyTrx(version);

    return getItem(rtx);
  }

  @Override
  public void delete() {
    try {
      final Path databaseFile = database.getDatabaseConfig().getDatabaseFile();
      database.close();
      jsonDbStore.removeDatabase(database);
      Databases.removeDatabase(databaseFile);
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
      final JsonResourceSession manager = database.beginResourceSession(resources.get(0).getFileName().toString());
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

  public JsonDBItem add(final String resourceName, final JsonReader reader, final Object options) {
    try {
      String resName = resourceName;
      for (final Path resource : database.listResources()) {
        final String existingResourceName = resource.getFileName().toString();
        if (existingResourceName.equals(resourceName)) {
          resName = existingResourceName + "1";
          break;
        }
      }

      final Sequence commitMessageSequence = options.get(new QNm("commitMessage"));
      final Sequence dateTimeSequence = options.get(new QNm("commitTimestamp"));

      final String commitMessage = commitMessageSequence != null ? ((Str) commitMessageSequence).stringValue() : null;
      final Instant commitTimestamp = dateTimeSequence != null ? dateTimeToInstant.convert(new DateTime(dateTimeSequence.toString())) : null;

      database.createResource(ResourceConfiguration.newBuilder(resName)
                                                   .useDeweyIDs(true)
                                                   .customCommitTimestamps(commitTimestamp != null)
                                                   .buildPathSummary(true)
                                                   .hashKind(HashType.ROLLING)
                                                   .build());
      final JsonResourceSession manager = database.beginResourceSession(resName);
      try (final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
        wtx.commit(commitMessage, commitTimestamp);
      }
      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx();
      rtx.moveToDocumentRoot();
      return getItem(rtx);
    } catch (final SirixException e) {
      LOG_WRAPPER.error(e.getMessage(), e);
      return null;
    }
  }

  public JsonDBItem add(final String resourceName, final JsonReader reader) {
    return add(resourceName, reader, new ArrayObject(new QNm[0], new Sequence[0]));
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
        final JsonResourceSession resource = database.beginResourceSession(resourceName);
        final JsonNodeReadOnlyTrx rtx = resource.beginNodeReadOnlyTrx();

        if (rtx.moveToFirstChild()) {
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
    requireNonNull(json);

    return add(JsonShredder.createStringReader(json), new ArrayObject(new QNm[0], new Sequence[0]));
  }

  @SuppressWarnings("SameParameterValue")
  private JsonDBItem add(final JsonReader reader, final Object options) {
    try {
      final String commitMessage = ((Str) options.get(new QNm("commitMessage"))).stringValue();
      final Sequence dateTime = options.get(new QNm("commitTimestamp"));

      //final String commitMessage = args.length >= 5 ? FunUtil.getString(args, 4, "commitMessage", null, null, false) : null;
      //final DateTime dateTime = args.length == 6 ? (DateTime) args[5] : null;
      final Instant commitTimestamp = dateTime != null ? dateTimeToInstant.convert((DateTime) dateTime) : null;

      final String resourceName = "resource" + (database.listResources().size() + 1);
      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useDeweyIDs(true)
                                                   .useTextCompression(true)
                                                   .buildPathSummary(true)
                                                   .customCommitTimestamps(commitTimestamp != null)
                                                   .hashKind(HashType.ROLLING)
                                                   .build());
      final JsonResourceSession manager = database.beginResourceSession(resourceName);
      try (final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader, JsonNodeTrx.Commit.NO);
        wtx.commit(commitMessage, commitTimestamp);
      }

      final JsonNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx();
      return getItem(rtx);
    } catch (final SirixException e) {
      LOG_WRAPPER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public JsonDBItem add(final Path file) {
    requireNonNull(file);

    return add(JsonShredder.createFileReader(file), new ArrayObject(new QNm[0], new Sequence[0]));
  }

}
