package org.sirix.xquery.node;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import javax.xml.stream.XMLEventReader;
import org.brackit.xquery.node.AbstractCollection;
import org.brackit.xquery.node.parser.CollectionParser;
import org.brackit.xquery.node.parser.SubtreeHandler;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.TemporalCollection;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.Transaction;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.xml.shredder.InsertPosition;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

/**
 * Database collection.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class XmlDBCollection extends AbstractCollection<AbstractTemporalNode<XmlDBNode>>
    implements TemporalCollection<AbstractTemporalNode<XmlDBNode>>, AutoCloseable {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(XmlDBCollection.class));

  /** ID sequence. */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /** {@link Sirix} database. */
  private final Database<XmlResourceManager> mDatabase;

  /** Unique ID. */
  private final int mID;

  /**
   * Constructor.
   *
   * @param name collection name
   * @param database Sirix {@link Database} reference
   */
  public XmlDBCollection(final String name, final Database<XmlResourceManager> database) {
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

    if (!(other instanceof XmlDBCollection))
      return false;

    final XmlDBCollection coll = (XmlDBCollection) other;
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
  public Database<XmlResourceManager> getDatabase() {
    return mDatabase;
  }

  @Override
  public XmlDBNode getDocument(Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  @Override
  public XmlDBNode getDocument(String name, Instant pointInTime) {
    return getDocumentInternal(name, pointInTime);
  }

  private XmlDBNode getDocumentInternal(final String resName, final Instant pointInTime) {
    final XmlResourceManager resource = mDatabase.openResourceManager(resName);

    XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(pointInTime);

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

    return new XmlDBNode(trx, this);
  }

  private XmlDBNode getDocumentInternal(final String resName, final int revision) {
    final XmlResourceManager resource = mDatabase.openResourceManager(resName);
    final int version = revision == -1
        ? resource.getMostRecentRevisionNumber()
        : revision;

    final XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx(version);

    return new XmlDBNode(trx, this);
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
  public XmlDBNode getDocument(final @Nonnegative int revision) {
    final List<Path> resources = mDatabase.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      final XmlResourceManager manager = mDatabase.openResourceManager(resources.get(0).getFileName().toString());
      final int version = revision == -1
          ? manager.getMostRecentRevisionNumber()
          : revision;
      final XmlNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);
      return new XmlDBNode(rtx, this);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  public XmlDBNode add(final String resName, SubtreeParser parser)
      throws OperationNotSupportedException, DocumentException {
    try {
      final String resource =
          new StringBuilder(2).append("resource").append(mDatabase.listResources().size() + 1).toString();
      mDatabase.createResource(ResourceConfiguration.newBuilder(resource)
                                                    .useDeweyIDs(true)
                                                    .useTextCompression(true)
                                                    .buildPathSummary(true)
                                                    .build());
      final XmlResourceManager manager = mDatabase.openResourceManager(resource);
      final XmlNodeTrx wtx = manager.beginNodeTrx();
      final SubtreeHandler handler =
          new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

      // Make sure the CollectionParser is used.
      if (!(parser instanceof CollectionParser)) {
        parser = new CollectionParser(parser);
      }

      parser.parse(handler);
      return new XmlDBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public XmlDBNode add(SubtreeParser parser) throws OperationNotSupportedException, DocumentException {
    try {
      final String resourceName =
          new StringBuilder(2).append("resource").append(mDatabase.listResources().size() + 1).toString();
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                    .useDeweyIDs(true)
                                                    .useTextCompression(true)
                                                    .buildPathSummary(true)
                                                    .build());
      final XmlResourceManager resource = mDatabase.openResourceManager(resourceName);
      final XmlNodeTrx wtx = resource.beginNodeTrx();

      final SubtreeHandler handler =
          new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

      // Make sure the CollectionParser is used.
      if (!(parser instanceof CollectionParser)) {
        parser = new CollectionParser(parser);
      }

      parser.parse(handler);
      return new XmlDBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  public XmlDBNode add(final String resourceName, final XMLEventReader reader) {
    try {
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName).useDeweyIDs(true).build());
      final XmlResourceManager resource = mDatabase.openResourceManager(resourceName);
      final XmlNodeTrx wtx = resource.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(reader);
      wtx.moveToDocumentRoot();
      return new XmlDBNode(wtx, this);
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
    final List<Path> resources = mDatabase.listResources();
    final List<XmlDBNode> documents = new ArrayList<>(resources.size());

    resources.forEach(resourcePath -> {
      try {
        final String resourceName = resourcePath.getFileName().toString();
        final XmlResourceManager resource = mDatabase.openResourceManager(resourceName);
        final XmlNodeReadOnlyTrx trx = resource.beginNodeReadOnlyTrx();
        documents.add(new XmlDBNode(trx, this));
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    });

    return new ArrayStream<>(documents.toArray(new XmlDBNode[documents.size()]));
  }
}
