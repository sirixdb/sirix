package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

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
import org.brackit.xquery.node.parser.SubtreeListener;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.TemporalCollection;
import org.sirix.access.Databases;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.Transaction;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Database collection.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class DBCollection extends AbstractCollection<AbstractTemporalNode<DBNode>>
		implements TemporalCollection<AbstractTemporalNode<DBNode>>, AutoCloseable {

	/** Logger. */
	private static final LogWrapper LOGGER =
			new LogWrapper(LoggerFactory.getLogger(DBCollection.class));

	/** ID sequence. */
	private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

	/** {@link Sirix} database. */
	private final Database mDatabase;

	/** Unique ID. */
	private final int mID;

	/**
	 * Constructor.
	 *
	 * @param name collection name
	 * @param database Sirix {@link Database} reference
	 */
	public DBCollection(final String name, final Database database) {
		super(checkNotNull(name));
		mDatabase = checkNotNull(database);
		mID = ID_SEQUENCE.incrementAndGet();
	}

	public Transaction beginTransaction() {
		return mDatabase.beginTransaction();
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof DBCollection) {
			final DBCollection coll = (DBCollection) obj;
			return mDatabase.equals(coll.mDatabase);
		}
		return false;
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
	public Database getDatabase() {
		return mDatabase;
	}

	@Override
	public void delete() throws DocumentException {
		try {
			Databases
					.truncateDatabase(new DatabaseConfiguration(mDatabase.getDatabaseConfig().getFile()));
		} catch (final SirixIOException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void remove(final long documentID)
			throws OperationNotSupportedException, DocumentException {
		if (documentID >= 0) {
			final String resource = mDatabase.getResourceName((int) documentID);
			if (resource != null) {
				mDatabase.truncateResource(resource);
			}
		}
	}

	@Override
	public DBNode getDocument(final @Nonnegative int revision) throws DocumentException {
		final String[] resources = mDatabase.listResources();
		if (resources.length > 1) {
			throw new DocumentException("More than one document stored in database/collection!");
		}
		try {
			final ResourceManager session = mDatabase
					.getResourceManager(ResourceManagerConfiguration.newBuilder(resources[0]).build());
			final int version = revision == -1 ? session.getMostRecentRevisionNumber() : revision;
			final XdmNodeReadTrx rtx = session.beginNodeReadTrx(version);
			return new DBNode(rtx, this);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	public DBNode add(final String resName, SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		try {
			final String resource = new StringBuilder(2).append("resource")
					.append(mDatabase.listResources().length + 1).toString();
			mDatabase
					.createResource(ResourceConfiguration.newBuilder(resource, mDatabase.getDatabaseConfig())
							.useDeweyIDs(true).useTextCompression(true).buildPathSummary(true).build());
			final ResourceManager manager =
					mDatabase.getResourceManager(ResourceManagerConfiguration.newBuilder(resource).build());
			final XdmNodeWriteTrx wtx = manager.beginNodeWriteTrx();
			final SubtreeHandler handler = new SubtreeBuilder(this, wtx, Insert.ASFIRSTCHILD,
					Collections.<SubtreeListener<? super AbstractTemporalNode<DBNode>>>emptyList());

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
			final String resourceName = new StringBuilder(2).append("resource")
					.append(mDatabase.listResources().length + 1).toString();
			mDatabase.createResource(
					ResourceConfiguration.newBuilder(resourceName, mDatabase.getDatabaseConfig())
							.useDeweyIDs(true).useTextCompression(true).buildPathSummary(true).build());
			final ResourceManager resource = mDatabase
					.getResourceManager(ResourceManagerConfiguration.newBuilder(resourceName).build());
			final XdmNodeWriteTrx wtx = resource.beginNodeWriteTrx();

			final SubtreeHandler handler = new SubtreeBuilder(this, wtx, Insert.ASFIRSTCHILD,
					Collections.<SubtreeListener<? super AbstractTemporalNode<DBNode>>>emptyList());

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
			mDatabase.createResource(ResourceConfiguration
					.newBuilder(resourceName, mDatabase.getDatabaseConfig()).useDeweyIDs(true).build());
			final ResourceManager resource = mDatabase
					.getResourceManager(ResourceManagerConfiguration.newBuilder(resourceName).build());
			final XdmNodeWriteTrx wtx = resource.beginNodeWriteTrx();
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
		return mDatabase.listResources().length;
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
	public DBNode getDocument(final int revision, final String name, final boolean updatable)
			throws DocumentException {
		try {
			final ResourceManagerConfiguration sessionConfig =
					ResourceManagerConfiguration.newBuilder(name).build();
			return getDocumentInternal(sessionConfig, revision, updatable);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	private DBNode getDocumentInternal(final ResourceManagerConfiguration resourceManagerConfig,
			final int revision, final boolean updatable) throws SirixException {
		final ResourceManager resource = mDatabase.getResourceManager(resourceManagerConfig);
		final int version = revision == -1 ? resource.getMostRecentRevisionNumber() : revision;

		final XdmNodeReadTrx trx;
		if (updatable) {
			if (resource.getAvailableNodeWriteTrx() == 0) {
				final Optional<XdmNodeWriteTrx> optionalWriteTrx;
				optionalWriteTrx = resource.getNodeWriteTrx();

				if (optionalWriteTrx.isPresent()) {
					trx = optionalWriteTrx.get();
				} else {
					trx = resource.beginNodeWriteTrx();
				}
			} else {
				trx = resource.beginNodeWriteTrx();
			}

			if (version < resource.getMostRecentRevisionNumber())
				((XdmNodeWriteTrx) trx).revertTo(version);
		} else {
			trx = resource.beginNodeReadTrx(version);
		}

		return new DBNode(trx, this);
	}

	@Override
	public DBNode getDocument(final int revision, final boolean updatable) throws DocumentException {
		final String[] resources = mDatabase.listResources();
		if (resources.length > 1) {
			throw new DocumentException("More than one document stored in database/collection!");
		}
		try {
			final ResourceManagerConfiguration sessionConfig =
					ResourceManagerConfiguration.newBuilder(resources[0]).build();

			return getDocumentInternal(sessionConfig, revision, updatable);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Stream<DBNode> getDocuments(final boolean updatable) throws DocumentException {
		final String[] resources = mDatabase.listResources();
		final List<DBNode> documents = new ArrayList<>(resources.length);
		for (final String resourceName : resources) {
			try {
				final ResourceManager resource = mDatabase
						.getResourceManager(ResourceManagerConfiguration.newBuilder(resourceName).build());
				final XdmNodeReadTrx trx =
						updatable ? resource.beginNodeWriteTrx() : resource.beginNodeReadTrx();
				documents.add(new DBNode(trx, this));
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return new ArrayStream<DBNode>(documents.toArray(new DBNode[documents.size()]));
	}

	@Override
	public DBNode getDocument(boolean updatabale) throws DocumentException {
		return getDocument(-1, updatabale);
	}
}
