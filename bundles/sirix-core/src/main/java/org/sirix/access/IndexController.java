package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.node.d2linked.D2NodeBuilder;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.util.serialize.SubtreePrinter;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Node;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.api.visitor.Visitor;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.ChangeListener;
import org.sirix.index.IndexBuilder;
import org.sirix.index.IndexDef;
import org.sirix.index.IndexType;
import org.sirix.index.Indexes;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.index.avltree.keyvalue.NodeReferences;
import org.sirix.index.cas.CASIndex;
import org.sirix.index.cas.CASIndexImpl;
import org.sirix.index.path.PathIndex;
import org.sirix.index.path.PathIndexImpl;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.Record;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * Index controller, used to control the handling of indexes.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class IndexController {

	/** Type of change. */
	public enum ChangeType {
		/** Insertion. */
		INSERT,

		/** Deletion. */
		DELETE
	}

	/** The index types. */
	private final Indexes mIndexes;

	/** Set of {@link ChangeListener}. */
	private final Set<ChangeListener> mListeners;

	/** The {@link PathIndex} implementation used to provide path indexes. */
	private final PathIndexImpl<Long, NodeReferences> mPathIndex;

	/** The {@link CASIndex} implementation used to provide path indexes. */
	private final CASIndexImpl<CASValue, NodeReferences> mCASIndex;

	/**
	 * Constructor.
	 */
	public IndexController() {
		mIndexes = new Indexes();
		mListeners = new HashSet<>();
		mPathIndex = new PathIndexImpl<Long, NodeReferences>();
		mCASIndex = new CASIndexImpl<CASValue, NodeReferences>();
	}
	
	public Indexes getIndexes() {
		return mIndexes;
	}

	/**
	 * Serialize to an {@link OutputStream}.
	 * 
	 * @param out
	 *          the {@link OutputStream} to serialize to
	 * @throws SirixException
	 *           if an exception occurs during serialization
	 */
	public void serialize(final @Nonnull OutputStream out) throws SirixException {
		try {
			new SubtreePrinter(new PrintStream(checkNotNull(out))).print(mIndexes
					.materialize());
		} catch (final DocumentException e) {
			throw new SirixException(e);
		}
	}
	
	/**
	 * Deserialize from an {@link InputStream}.
	 * 
	 * @param out
	 *          the {@link InputStream} from which to deserialize the XML fragment
	 * @throws SirixException
	 *           if an exception occurs during serialization
	 */
	public Node<?> deserialize(final @Nonnull InputStream in) throws SirixException {
		try {
			final DocumentParser parser = new DocumentParser(in);
			final D2NodeBuilder builder = new D2NodeBuilder();
			parser.parse(builder);
			return builder.root();
		} catch (final DocumentException e) {
			throw new SirixException(e);
		}
	}

	/**
	 * Notify the changes to all listening indexes.
	 * 
	 * @param type
	 *          type of change
	 * @param node
	 *          the node which has changed (either was inserted or deleted)
	 * @param pathNodeKey
	 *          the path node key of the node (might also be the path node key of
	 *          the parent node)
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	public void notifyChange(@Nonnull ChangeType type,
			@Nonnull ImmutableNode node, long pathNodeKey) throws SirixIOException {
		for (final ChangeListener listener : mListeners) {
			listener.listen(type, node, pathNodeKey);
		}
	}

	/**
	 * Create new indexes.
	 * 
	 * @param indexDefs
	 *          Set of {@link IndexDef}s
	 * @param nodeWriteTrx
	 *          the {@link NodeWriteTrx} used
	 * @return this {@link IndexController} instance
	 */
	public IndexController createIndexes(final @Nonnull Set<IndexDef> indexDefs,
			final @Nonnull NodeWriteTrx nodeWriteTrx) {
		// Index builders for all index definitions.
		final Set<Visitor> indexBuilders = new HashSet<>(indexDefs.size());
		for (final IndexDef indexDef : indexDefs) {
			switch (indexDef.getType()) {
			case PATH:
				indexBuilders.add(createPathIndexBuilder(
						nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(),
						indexDef));
				break;
			case CAS:
				indexBuilders.add(createCASIndexBuilder(nodeWriteTrx,
						nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(),
						indexDef));
			case NAME:
				// TODO:
				break;
			default:
				break;
			}
		}

		// Build the indexes.
		new IndexBuilder(nodeWriteTrx, indexBuilders).build();

		// Build and create index listeners.
		return createIndexListeners(indexDefs, nodeWriteTrx);
	}
	
	public IndexController createIndexListeners(final @Nonnull Set<IndexDef> indexDefs,
			final @Nonnull NodeWriteTrx nodeWriteTrx) {
		checkNotNull(nodeWriteTrx);
		// Save for upcoming modifications.
		for (final IndexDef indexDef : indexDefs) {
			mIndexes.add(indexDef);
			switch (indexDef.getType()) {
			case PATH:
				mListeners.add(createPathIndexListener(
						nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(),
						indexDef));
				break;
			case CAS:
				mListeners.add(createCASIndexListener(
						nodeWriteTrx.getPageTransaction(), nodeWriteTrx.getPathSummary(),
						indexDef));
			case NAME:
				// TODO:
				break;
			default:
				break;
			}
		}
		return this;
	}

	private ChangeListener createPathIndexListener(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnull PathSummaryReader pathSummaryReader,
			final @Nonnull IndexDef indexDef) {
		return mPathIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	private ChangeListener createCASIndexListener(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnull PathSummaryReader pathSummaryReader,
			final @Nonnull IndexDef indexDef) {
		return mCASIndex.createListener(pageWriteTrx, pathSummaryReader, indexDef);
	}

	private Visitor createPathIndexBuilder(
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnull PathSummaryReader pathSummaryReader,
			final @Nonnull IndexDef indexDef) {
		return mPathIndex.createBuilder(pageWriteTrx, pathSummaryReader, indexDef);
	}

	private Visitor createCASIndexBuilder(
			final @Nonnull NodeReadTrx nodeReadTrx,
			final @Nonnull PageWriteTrx<Long, Record, UnorderedKeyValuePage> pageWriteTrx,
			final @Nonnull PathSummaryReader pathSummaryReader,
			final @Nonnull IndexDef indexDef) {
		return mCASIndex.createBuilder(nodeReadTrx, pageWriteTrx,
				pathSummaryReader, indexDef);
	}

	/**
	 * Determines if an index of the specified type is available.
	 * 
	 * @param type
	 *          type of index to lookup
	 * @return {@code true} if an index of the specified type exists,
	 *         {@code false} otherwise
	 */
	public boolean containsIndex(IndexType type) {
		for (final IndexDef indexDef : mIndexes.getIndexDefs()) {
			if (indexDef.getType() == type) return true;
		}
		return false;
	}

}
