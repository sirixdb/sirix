package org.sirix.xquery.node;

import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.Stream;
import org.brackit.xquery.jdm.node.AbstractTemporalNode;
import org.brackit.xquery.jdm.node.TemporalNodeCollection;
import org.brackit.xquery.node.AbstractNodeCollection;
import org.brackit.xquery.node.parser.CollectionParser;
import org.brackit.xquery.node.parser.NodeSubtreeHandler;
import org.brackit.xquery.node.parser.NodeSubtreeParser;
import org.brackit.xquery.node.stream.ArrayStream;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.InsertPosition;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Objects.requireNonNull;

/**
 * Database collection.
 *
 * @author Johannes Lichtenberger
 */
public final class XmlDBCollection extends AbstractNodeCollection<AbstractTemporalNode<XmlDBNode>>
    implements TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>>, AutoCloseable {

  /**
   * Logger.
   */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(XmlDBCollection.class));

  /**
   * ID sequence.
   */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /**
   * {@link Database} instance.
   */
  private final Database<XmlResourceSession> database;

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
   * Constructor.
   *
   * @param name     collection name
   * @param database Sirix {@link Database} reference
   */
  public XmlDBCollection(final String name, final Database<XmlResourceSession> database) {
    super(requireNonNull(name));
    this.database = requireNonNull(database);
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
      final XmlResourceSession resource = database.beginResourceSession(resName);

      XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(pointInTime);

      if (trx.getRevisionTimestamp().isAfter(pointInTime)) {
        final int revision = trx.getRevisionNumber();

        trx.close();
        if (revision > 1) {
          trx = resource.beginNodeReadOnlyTrx(revision - 1);
        } else {
          return null;
        }
      }

      return new XmlDBNode(trx, this);
    });
  }

  private XmlDBNode getDocumentInternal(final String resName, final int revision) {
    if (revision == -1) {
      return createXmlDBNode(revision, resName);
    } else {
      return documentDataToXmlDBNodes.computeIfAbsent(new DocumentData(resName, revision),
                                                      (unused) -> createXmlDBNode(revision, resName));
    }
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
      if (revision == -1) {
        return createXmlDBNode(revision, resourceName);
      } else {
        return documentDataToXmlDBNodes.computeIfAbsent(new DocumentData(resourceName, revision),
                                                        (unused) -> createXmlDBNode(revision, resourceName));
      }
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private XmlDBNode createXmlDBNode(int revision, @NonNull String resourceName) {
    final XmlResourceSession manager = database.beginResourceSession(resourceName);
    final int version = revision == -1 ? manager.getMostRecentRevisionNumber() : revision;
    final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);
    return new XmlDBNode(rtx, this);
  }

  public XmlDBNode add(final String resourceName, NodeSubtreeParser parser) {
    try {
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  public XmlDBNode add(final String resourceName, final NodeSubtreeParser parser, final String commitMessage,
      final Instant commitTimestamp) {
    try {
      return createResource(parser, resourceName, commitMessage, commitTimestamp);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public XmlDBNode add(NodeSubtreeParser parser) throws DocumentException {
    try {
      final String resourceName = "resource" + (database.listResources().size() + 1);
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  private XmlDBNode createResource(NodeSubtreeParser parser, final String resourceName, final String commitMessage,
      final Instant commitTimestamp) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .useDeweyIDs(true)
                                                 .useTextCompression(true)
                                                 .buildPathSummary(true)
                                                 .customCommitTimestamps(commitTimestamp != null)
                                                 .hashKind(HashType.ROLLING)
                                                 .build());
    final XmlResourceSession resource = database.beginResourceSession(resourceName);
    final XmlNodeTrx wtx = resource.beginNodeTrx();

    final NodeSubtreeHandler handler =
        new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

    // Make sure the CollectionParser is used.
    if (!(parser instanceof CollectionParser)) {
      parser = new CollectionParser(parser);
    }

    parser.parse(handler);

    wtx.commit(commitMessage, commitTimestamp);

    final var xmlDBNode = new XmlDBNode(wtx, this);
    documentDataToXmlDBNodes.put(new DocumentData(resourceName, 1), xmlDBNode);
    instantDocumentDataToXmlDBNodes.put(new InstantDocumentData(resourceName, wtx.getRevisionTimestamp()), xmlDBNode);
    return xmlDBNode;
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
      try {
        final String resourceName = resourcePath.getFileName().toString();
        final XmlResourceSession resource = database.beginResourceSession(resourceName);
        final XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx();
        documents.add(new XmlDBNode(trx, this));
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    });

    return new ArrayStream<>(documents.toArray(new XmlDBNode[0]));
  }

  private record DocumentData(String name, int revision) {
  }

  private record InstantDocumentData(String name, Instant revision) {
  }
}
