package io.sirix.query.node;

import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.AbstractTemporalNode;
import io.brackit.query.node.AbstractNodeCollection;
import io.brackit.query.node.parser.CollectionParser;
import io.brackit.query.node.parser.NodeSubtreeHandler;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.brackit.query.node.stream.ArrayStream;
import org.jspecify.annotations.Nullable;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.exception.SirixIOException;
import io.sirix.service.InsertPosition;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Standard {@link XmlDBCollection} implementation backed by a Sirix {@link Database}.
 *
 * <p>
 * {@link XmlDBCollection} is an interface so cross-cutting concerns (e.g. the REST layer's
 * per-request authorization, see {@code AuthCheckingXmlDBCollection}) can be layered on by
 * composition/delegation rather than by subclassing this class. equals/hashCode key on the database
 * only, so a delegating wrapper over the same database compares equal to the original.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlDBCollectionImpl extends AbstractNodeCollection<AbstractTemporalNode<XmlDBNode>>
    implements XmlDBCollection {

  /**
   * Logger.
   */
  private static final LogWrapper LOG_WRAPPER = new LogWrapper(LoggerFactory.getLogger(XmlDBCollectionImpl.class));

  /**
   * ID sequence.
   */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /**
   * {@link Database} instance.
   */
  private final Database<XmlResourceSession> database;

  private final XmlResourceOptions resourceOptions;

  /**
   * Unique ID.
   */
  private final int id;

  /**
   * "Caches" document nodes.
   */
  private final Map<DocumentData, XmlDBNode> documentDataToXmlDBNodes;

  /**
   * "Caches" document nodes.
   */
  private final Map<InstantDocumentData, XmlDBNode> instantDocumentDataToXmlDBNodes;

  /**
   * Construct a collection with resource defaults from {@link BasicXmlDBStore#newBuilder()}.
   *
   * @param name collection name
   * @param database Sirix {@link Database} reference
   */
  public XmlDBCollectionImpl(final String name, final Database<XmlResourceSession> database) {
    this(name, database, BasicXmlDBStore.newBuilder().resourceOptions());
  }

  XmlDBCollectionImpl(final String name, final Database<XmlResourceSession> database,
      final XmlResourceOptions resourceOptions) {
    super(requireNonNull(name));
    this.database = requireNonNull(database);
    this.resourceOptions = requireNonNull(resourceOptions);
    id = ID_SEQUENCE.incrementAndGet();
    documentDataToXmlDBNodes = new HashMap<>();
    instantDocumentDataToXmlDBNodes = new HashMap<>();
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (this == other)
      return true;

    if (!(other instanceof final XmlDBCollection coll))
      return false;

    return database.equals(coll.getDatabase());
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
  public Database<XmlResourceSession> getDatabase() {
    return database;
  }

  @Override
  public XmlDBNode getDocument(Instant pointInTime) {
    final List<Path> resources = database.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }

    try {
      final var resourceName = resources.get(0).getFileName().toString();
      return getDocumentInternal(resourceName, pointInTime);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public XmlDBNode getDocument(String name, Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  private XmlDBNode getDocumentInternal(final String resName, final Instant pointInTime) {
    return instantDocumentDataToXmlDBNodes.computeIfAbsent(new InstantDocumentData(resName, pointInTime), (unused) -> {
      // Borrowed-session ownership is defined by Database.beginResourceSession: close only our trx.
      final XmlResourceSession resource = database.beginResourceSession(resName);
      XmlNodeReadOnlyTrx trx = null;
      try {
        trx = resource.beginNodeReadOnlyTrx(pointInTime);

        if (trx.getRevisionNumber() == 0) {
          // Revision 0 is the internal empty state, before the first committed document.
          trx.close();
          return null;
        }

        if (trx.getRevisionTimestamp().isAfter(pointInTime)) {
          final int revision = trx.getRevisionNumber();

          if (revision > 1) {
            trx.close();

            trx = resource.beginNodeReadOnlyTrx(revision - 1);
          } else {
            // Even the earliest revision was committed after the requested point
            // in time, i.e. the resource did not exist yet. Return the empty
            // sequence instead of anachronistically yielding the first revision.
            // (Returning null from computeIfAbsent leaves the cache untouched.)
            trx.close();
            return null;
          }
        }

        return new XmlDBNode(new ThreadSafeXmlReadOnlyTrx(trx), this);
      } catch (final Exception e) {
        if (trx != null && !trx.isClosed()) {
          trx.close();
        }
        throw e;
      }
    });
  }

  private XmlDBNode getDocumentInternal(final String resName, final int revision) {
    return revision == -1
        ? createXmlDBNode(revision, resName)
        : documentDataToXmlDBNodes.computeIfAbsent(new DocumentData(resName, revision),
            (unused) -> createXmlDBNode(revision, resName));
  }

  @Override
  public void delete() {
    try {
      final Path databaseFile = database.getDatabaseConfig().getDatabaseFile();
      database.close();
      Databases.removeDatabase(databaseFile);
    } catch (final SirixIOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void remove(final long documentID) {
    if (documentID >= 0) {
      final String resourceName = database.getResourceName((int) documentID);
      if (resourceName != null) {
        database.removeResource(resourceName);

        removeFromDocumentDataToXmlDBNodes(resourceName);
        removeFromInstantDocumentDataToXmlDBNode(resourceName);
      }
    }
  }

  private void removeFromInstantDocumentDataToXmlDBNode(String resourceName) {
    instantDocumentDataToXmlDBNodes.keySet().removeIf(documentData -> documentData.name.equals(resourceName));
  }

  private void removeFromDocumentDataToXmlDBNodes(String resourceName) {
    documentDataToXmlDBNodes.keySet().removeIf(documentData -> documentData.name.equals(resourceName));
  }

  @Override
  public XmlDBNode getDocument(final int revision) {
    final List<Path> resources = database.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      final var resourceName = resources.get(0).getFileName().toString();
      return getDocumentInternal(resourceName, revision);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private XmlDBNode createXmlDBNode(int revision, String resourceName) {
    final XmlResourceSession resourceSession = database.beginResourceSession(resourceName);
    try {
      final int version = revision == -1
          ? resourceSession.getMostRecentRevisionNumber()
          : revision;
      final XmlNodeReadOnlyTrx rtx = resourceSession.beginNodeReadOnlyTrx(version);
      return new XmlDBNode(new ThreadSafeXmlReadOnlyTrx(rtx), this);
    } catch (final Exception e) {
      resourceSession.close();
      throw e;
    }
  }

  public XmlDBNode add(final String resourceName, NodeSubtreeParser parser) {
    try {
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOG_WRAPPER.error(e.getMessage(), e);
      return null;
    }
  }

  public XmlDBNode add(final String resourceName, final NodeSubtreeParser parser, final String commitMessage,
      final Instant commitTimestamp) {
    try {
      return createResource(parser, resourceName, commitMessage, commitTimestamp);
    } catch (final SirixException e) {
      LOG_WRAPPER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public XmlDBNode add(NodeSubtreeParser parser) throws DocumentException {
    try {
      final String resourceName = "resource" + (database.listResources().size() + 1);
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOG_WRAPPER.error(e.getMessage(), e);
      return null;
    }
  }

  private XmlDBNode createResource(NodeSubtreeParser parser, final String resourceName, final String commitMessage,
      final Instant commitTimestamp) {
    database.createResource(resourceOptions.create(resourceName, commitTimestamp));
    final XmlResourceSession resource = database.beginResourceSession(resourceName);
    final XmlNodeTrx wtx;
    try {
      wtx = resource.beginNodeTrx();
    } catch (final Exception e) {
      resource.close();
      throw e;
    }

    try {
      final NodeSubtreeHandler handler =
          new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

      // Make sure the CollectionParser is used.
      if (!(parser instanceof CollectionParser)) {
        parser = new CollectionParser(parser);
      }

      parser.parse(handler);

      wtx.commit(commitMessage, commitTimestamp);

      // Historical caches must use revision-specific readers, never the mutable writer.
      return new XmlDBNode(wtx, this);
    } catch (final Exception e) {
      wtx.close();
      resource.close();
      throw e;
    }
  }

  @Override
  public void close() {
    database.close();
  }

  @Override
  public long getDocumentCount() {
    return database.listResources().size();
  }

  @Override
  public XmlDBNode getDocument() {
    return getDocument(-1);
  }

  @Override
  public XmlDBNode getDocument(final String name, final int revision) {
    return getDocumentInternal(name, revision);
  }

  @Override
  public XmlDBNode getDocument(final String name) {
    return getDocument(name, -1);
  }

  @Override
  public Stream<XmlDBNode> getDocuments() {
    final List<Path> resources = database.listResources();
    final List<XmlDBNode> documents = new ArrayList<>(resources.size());

    resources.forEach(resourcePath -> {
      final String resourceName = resourcePath.getFileName().toString();
      final XmlResourceSession resource = database.beginResourceSession(resourceName);
      try {
        final XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx();
        documents.add(new XmlDBNode(trx, this));
      } catch (final SirixException e) {
        resource.close();
        throw new DocumentException(e.getCause());
      } catch (final Exception e) {
        resource.close();
        throw e;
      }
    });

    return new ArrayStream<>(documents.toArray(new XmlDBNode[0]));
  }

  private record DocumentData(String name, int revision) {
  }

  private record InstantDocumentData(String name, Instant revision) {
  }
}
