package org.sirix.xquery.node;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
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
public final class DBCollection extends AbstractCollection<AbstractTemporalNode<DBNode>>
    implements TemporalCollection<AbstractTemporalNode<DBNode>>, AutoCloseable {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(DBCollection.class));

  /** ID sequence. */
  private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

  /** {@link Sirix} database. */
  private final Database<XdmResourceManager> mDatabase;

  /** Unique ID. */
  private final int mID;

  /**
   * Constructor.
   *
   * @param name collection name
   * @param database Sirix {@link Database} reference
   */
  public DBCollection(final String name, final Database<XdmResourceManager> database) {
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

    if (!(other instanceof DBCollection))
      return false;

    final DBCollection coll = (DBCollection) other;
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
  public Database<XdmResourceManager> getDatabase() {
    return mDatabase;
  }

  @Override
  public DBNode getDocument(Instant pointInTime) throws DocumentException {
    return getDocument(pointInTime, name, false);
  }

  @Override
  public DBNode getDocument(Instant pointInTime, boolean updatable) throws DocumentException {
    return getDocument(pointInTime, name, updatable);
  }

  @Override
  public DBNode getDocument(Instant pointInTime, String name) throws DocumentException {
    return getDocument(pointInTime, name, false);
  }

  @Override
  public DBNode getDocument(Instant pointInTime, String name, boolean updatable) throws DocumentException {
    try {
      return getDocumentInternal(name, pointInTime, updatable);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private DBNode getDocumentInternal(final String resName, final Instant pointInTime, final boolean updatable) {
    final XdmResourceManager resource = mDatabase.getResourceManager(resName);

    final XdmNodeReadOnlyTrx trx;

    if (updatable) {
      if (resource.hasRunningNodeWriteTrx()) {
        final Optional<XdmNodeTrx> optionalWriteTrx = resource.getNodeWriteTrx();

        if (optionalWriteTrx.isPresent()) {
          trx = optionalWriteTrx.get();
        } else {
          trx = resource.beginNodeTrx();
        }
      } else {
        trx = resource.beginNodeTrx();
      }

      final int revision = resource.getRevisionNumber(pointInTime);

      if (revision < resource.getMostRecentRevisionNumber())
        ((XdmNodeTrx) trx).revertTo(revision);
    } else {
      trx = resource.beginNodeReadOnlyTrx(pointInTime);
    }

    return new DBNode(trx, this);
  }

  @Override
  public void delete() throws DocumentException {
    try {
      Databases.removeDatabase(mDatabase.getDatabaseConfig().getFile());
    } catch (final SirixIOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void remove(final long documentID) throws OperationNotSupportedException, DocumentException {
    if (documentID >= 0) {
      final String resource = mDatabase.getResourceName((int) documentID);
      if (resource != null) {
        mDatabase.removeResource(resource);
      }
    }
  }

  @Override
  public DBNode getDocument(final @Nonnegative int revision) throws DocumentException {
    final List<Path> resources = mDatabase.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      final XdmResourceManager manager = mDatabase.getResourceManager(resources.get(0).getFileName().toString());
      final int version = revision == -1
          ? manager.getMostRecentRevisionNumber()
          : revision;
      final XdmNodeReadOnlyTrx rtx = manager.beginNodeReadOnlyTrx(version);
      return new DBNode(rtx, this);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  public DBNode add(final String resName, SubtreeParser parser)
      throws OperationNotSupportedException, DocumentException {
    try {
      final String resource =
          new StringBuilder(2).append("resource").append(mDatabase.listResources().size() + 1).toString();
      mDatabase.createResource(ResourceConfiguration.newBuilder(resource)
                                                    .useDeweyIDs(true)
                                                    .useTextCompression(true)
                                                    .buildPathSummary(true)
                                                    .build());
      final XdmResourceManager manager = mDatabase.getResourceManager(resource);
      final XdmNodeTrx wtx = manager.beginNodeTrx();
      final SubtreeHandler handler =
          new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

      // Make sure the CollectionParser is used.
      if (!(parser instanceof CollectionParser)) {
        parser = new CollectionParser(parser);
      }

      parser.parse(handler);
      return new DBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public DBNode add(SubtreeParser parser) throws OperationNotSupportedException, DocumentException {
    try {
      final String resourceName =
          new StringBuilder(2).append("resource").append(mDatabase.listResources().size() + 1).toString();
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                    .useDeweyIDs(true)
                                                    .useTextCompression(true)
                                                    .buildPathSummary(true)
                                                    .build());
      final XdmResourceManager resource = mDatabase.getResourceManager(resourceName);
      final XdmNodeTrx wtx = resource.beginNodeTrx();

      final SubtreeHandler handler =
          new SubtreeBuilder(this, wtx, InsertPosition.AS_FIRST_CHILD, Collections.emptyList());

      // Make sure the CollectionParser is used.
      if (!(parser instanceof CollectionParser)) {
        parser = new CollectionParser(parser);
      }

      parser.parse(handler);
      return new DBNode(wtx, this);
    } catch (final SirixException e) {
      LOGGER.error(e.getMessage(), e);
      return null;
    }
  }

  public DBNode add(final String resourceName, final XMLEventReader reader)
      throws OperationNotSupportedException, DocumentException {
    try {
      mDatabase.createResource(ResourceConfiguration.newBuilder(resourceName).useDeweyIDs(true).build());
      final XdmResourceManager resource = mDatabase.getResourceManager(resourceName);
      final XdmNodeTrx wtx = resource.beginNodeTrx();
      wtx.insertSubtreeAsFirstChild(reader);
      wtx.moveToDocumentRoot();
      return new DBNode(wtx, this);
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
  public DBNode getDocument() throws DocumentException {
    return getDocument(-1);
  }

  @Override
  public Stream<DBNode> getDocuments() throws DocumentException {
    return getDocuments(false);
  }

  @Override
  public DBNode getDocument(final int revision, final String name) throws DocumentException {
    return getDocument(revision, name, false);
  }

  @Override
  public DBNode getDocument(final String name) throws DocumentException {
    return getDocument(-1, name, false);
  }

  @Override
  public DBNode getDocument(final int revision, final String name, final boolean updatable) throws DocumentException {
    try {
      return getDocumentInternal(name, revision, updatable);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private DBNode getDocumentInternal(final String resName, final int revision, final boolean updatable) {
    final XdmResourceManager resource = mDatabase.getResourceManager(resName);
    final int version = revision == -1
        ? resource.getMostRecentRevisionNumber()
        : revision;

    final XdmNodeReadOnlyTrx trx;
    if (updatable) {
      if (resource.hasRunningNodeWriteTrx()) {
        final Optional<XdmNodeTrx> optionalWriteTrx = resource.getNodeWriteTrx();

        if (optionalWriteTrx.isPresent()) {
          trx = optionalWriteTrx.get();
        } else {
          trx = resource.beginNodeTrx();
        }
      } else {
        trx = resource.beginNodeTrx();
      }

      if (version < resource.getMostRecentRevisionNumber())
        ((XdmNodeTrx) trx).revertTo(version);
    } else {
      trx = resource.beginNodeReadOnlyTrx(version);
    }

    return new DBNode(trx, this);
  }

  @Override
  public DBNode getDocument(final int revision, final boolean updatable) throws DocumentException {
    final List<Path> resources = mDatabase.listResources();
    if (resources.size() > 1) {
      throw new DocumentException("More than one document stored in database/collection!");
    }
    try {
      return getDocumentInternal(resources.get(0).getFileName().toString(), revision, updatable);
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public Stream<DBNode> getDocuments(final boolean updatable) throws DocumentException {
    final List<Path> resources = mDatabase.listResources();
    final List<DBNode> documents = new ArrayList<>(resources.size());

    // Foreach because of throwing of an unchecked exception (DocumentException).
    for (final Path resourcePath : resources) {
      try {
        final String resourceName = resourcePath.getFileName().toString();
        final XdmResourceManager resource = mDatabase.getResourceManager(resourceName);
        final XdmNodeReadOnlyTrx trx = updatable
            ? resource.beginNodeTrx()
            : resource.beginNodeReadOnlyTrx();
        documents.add(new DBNode(trx, this));
      } catch (final SirixException e) {
        throw new DocumentException(e.getCause());
      }
    }

    return new ArrayStream<>(documents.toArray(new DBNode[documents.size()]));
  }

  @Override
  public DBNode getDocument(boolean updatabale) throws DocumentException {
    return getDocument(-1, updatabale);
  }
}
