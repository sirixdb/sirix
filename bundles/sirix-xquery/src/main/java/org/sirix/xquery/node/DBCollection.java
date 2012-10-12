package org.sirix.xquery.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import org.brackit.xquery.node.AbstractCollection;
import org.brackit.xquery.node.parser.SubtreeParser;
import org.brackit.xquery.node.stream.ArrayStream;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.OperationNotSupportedException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;

/**
 * Database collection.
 * 
 * @author Johannes Lichtenberger
 * 
 * @param <E>
 *          generic parameter, usually a {@link DBNode}
 */
public class DBCollection<E extends AbsTemporalNode> extends
		AbstractCollection<AbsTemporalNode> implements AutoCloseable {

	/** ID sequence. */
	private static final AtomicInteger ID_SEQUENCE = new AtomicInteger();

	/** {@link Sirix} database. */
	private final IDatabase mDatabase;

	/** Determines if collection needs to be updatable. */
	private final boolean mUpdating;

	/** Unique ID. */
	private final int mID;

	/**
	 * Constructor.
	 * 
	 * @param pName
	 *          collection name
	 * @param pDatabase
	 *          Sirix {@link IDatabase} reference
	 */
	public DBCollection(final @Nonnull String pName,
			final @Nonnull IDatabase pDatabase, final boolean pUpdating) {
		super(checkNotNull(pName));
		mDatabase = checkNotNull(pDatabase);
		mUpdating = pUpdating;
		mID = ID_SEQUENCE.incrementAndGet();
	}

	/**
	 * Get the unique ID.
	 * 
	 * @return unique ID
	 */
	public int getID() {
		return mID;
	}

	@Override
	public void delete() throws DocumentException {
		try {
			Database.truncateDatabase(new DatabaseConfiguration(mDatabase
					.getDatabaseConfig().getFile()));
		} catch (final SirixIOException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public void remove(final long documentID)
			throws OperationNotSupportedException, DocumentException {
		if (documentID >= 0) {
			final String documentName = mDatabase.getResourceName((int) documentID);
			if (documentName != null) {
				mDatabase.truncateResource(mDatabase.getResourceName((int) documentID));
			}
		}
	}

	@Override
	public AbsTemporalNode getDocument() throws DocumentException {
		final String[] resources = mDatabase.listResources();
		if (resources.length > 1) {
			throw new DocumentException("More than one document stored!");
		}
		try {
			final ISession session = mDatabase
					.getSession(new SessionConfiguration.Builder(resources[0]).build());
			final INodeReadTrx rtx = mUpdating ? session.beginNodeWriteTrx()
					: session.beginNodeReadTrx();
			return new DBNode(rtx, this);
		} catch (final SirixException e) {
			throw new DocumentException(e.getCause());
		}
	}

	@Override
	public Stream<? extends AbsTemporalNode> getDocuments()
			throws DocumentException {
		final String[] resources = mDatabase.listResources();
		final List<DBNode> documents = new ArrayList<>(resources.length);
		for (final String resource : resources) {
			try {
				final ISession session = mDatabase
						.getSession(new SessionConfiguration.Builder(resource).build());
				final INodeReadTrx rtx = mUpdating ? session.beginNodeWriteTrx()
						: session.beginNodeReadTrx();
				documents.add(new DBNode(rtx, this));
			} catch (final SirixException e) {
				throw new DocumentException(e.getCause());
			}
		}
		return new ArrayStream<AbsTemporalNode>(
				documents.toArray(new DBNode[documents.size()]));
	}

	@Override
	public AbsTemporalNode add(final SubtreeParser parser)
			throws OperationNotSupportedException, DocumentException {
		return null;
	}

	@Override
	public void close() throws SirixException {
		mDatabase.close();
	}
}
