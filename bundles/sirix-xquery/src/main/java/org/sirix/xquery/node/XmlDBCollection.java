package org.sirix.xquery.node;

import com.google.common.base.Preconditions;
import org.brackit.xquery.node.AbstractNodeCollection;
import org.brackit.xquery.node.parser.CollectionParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.node.AbstractTemporalNode;
import org.brackit.xquery.xdm.node.TemporalNodeCollection;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.InsertPosition;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final Database<XmlResourceManager> database;

  /**
   * Unique ID.
   */
  private final int id;

  /**
   * "Caches" document nodes.
   */
  private Map<DocumentData, XmlDBNode> documentDataToXmlDBNodes;

  /**
   * "Caches" document nodes.
   */
  private Map<InstantDocumentData, XmlDBNode> instantDocumentDataToXmlDBNodes;

  /**
   * Constructor.
   *
   * @param name     collection name
   * @param database Sirix {@link Database} reference
   */
  public XmlDBCollection(final String name, final Database<XmlResourceManager> database) {
    super(Preconditions.checkNotNull(name));
    this.database = Preconditions.checkNotNull(database);
    id = ID_SEQUENCE.incrementAndGet();
    documentDataToXmlDBNodes = new HashMap<>();
    instantDocumentDataToXmlDBNodes = new HashMap<>();
  }

  @Override
  public boolean equals(final @Nullable Object other) {
    if (this == other)
      return true;

    if (!(other instanceof XmlDBCollection))
      return false;

    final XmlDBCollection coll = (XmlDBCollection) other;
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
  public Database<XmlResourceManager> getDatabase() {
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
      final XmlResourceManager resource = database.openResourceManager(resName);

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

  private XmlDBNode createXmlDBNode(int revision, @Nonnull String resourceName) {
    final XmlResourceManager manager = database.openResourceManager(resourceName);
    final int version = revision == -1 ? manager.getMostRecentRevisionNumber() : revision;
    final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);
    return new XmlDBNode(rtx, this);
  }

  public XmlDBNode add(final String resourceName, SubtreeParser parser) {
    try {
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  public XmlDBNode add(final String resourceName, final SubtreeParser parser, final String commitMessage,
      final Instant commitTimestamp) {
    try {
      return createResource(parser, resourceName, commitMessage, commitTimestamp);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public XmlDBNode add(SubtreeParser parser) throws DocumentException {
    try {
      final String resourceName = "resource" + (database.listResources().size() + 1);
      return createResource(parser, resourceName, null, null);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  private XmlDBNode createResource(SubtreeParser parser, final String resourceName, final String commitMessage,
      final Instant commitTimestamp) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .useDeweyIDs(true)
                                                 .useTextCompression(true)
                                                 .buildPathSummary(true)
                                                 .customCommitTimestamps(commitTimestamp != null)
                                                 .build());
    final XmlResourceManager resource = database.openResourceManager(resourceName);
    final XmlNodeTrx wtx = resource.beginNodeTrx();

    final SubtreeHandler handler =
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

  //  public XmlDBNode add(final String resourceName, final XMLEventReader reader) {
  //    try {
  //      database.createResource(ResourceConfiguration.newBuilder(resourceName).useDeweyIDs(true).build());
  //      final XmlResourceManager resource = database.openResourceManager(resourceName);
  //      final XmlNodeTrx wtx = resource.beginNodeTrx();
  //      wtx.insertSubtreeAsFirstChild(reader);
  //      wtx.moveToDocumentRoot();
  //      return new XmlDBNode(wtx, this);
  //    } catch (final SirixException e) {
  //      LOGGER.error(e.getMessage(), e);
  //      return null;
  //    }
  //  }

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
        final XmlResourceManager resource = database.openResourceManager(resourceName);
        final XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx();
        documents.add(new XmlDBNode(trx, this));
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    });

    return new ArrayStream<>(documents.toArray(new XmlDBNode[0]));
  }

  private static final class DocumentData {
    final String name;

    final int revision;

    DocumentData(String name, int revision) {
      this.name = name;
      this.revision = revision;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other == null || getClass() != other.getClass())
        return false;
      DocumentData that = (DocumentData) other;
      return revision == that.revision && name.equals(that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, revision);
    }

    public String getName() {
      return name;
    }

    public int getRevision() {
      return revision;
    }
  }

  private static final class InstantDocumentData {
    final String name;

    final Instant revision;

    InstantDocumentData(String name, Instant revision) {
      this.name = name;
      this.revision = revision;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other == null || getClass() != other.getClass())
        return false;
      InstantDocumentData that = (InstantDocumentData) other;
      return revision.equals(that.revision) && name.equals(that.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, revision);
    }
  }
}
