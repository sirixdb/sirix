package org.sirix.page;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.sirix.api.PageWriteTrx;
import org.sirix.cache.IndirectPageLogKey;
import org.sirix.cache.RecordPageContainer;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Fixed;

/**
 * Page utilities.
 * 
 * @author Johannes Lichtenberger
 *
 */
public final class PageUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private PageUtils() {
		throw new AssertionError("May never be instantiated!");
	}

	/**
	 * Create the initial tree structure.
	 * 
	 * @param reference
	 *          reference from revision root
	 * @param pageKind
	 *          the page kind
	 */
	public static <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void createTree(
			@Nonnull PageReference reference, final PageKind pageKind,
			final int index, final PageWriteTrx<K, V, S> pageWriteTrx) {
		Page page = null;

		// Level page count exponent from the configuration.
		final int[] levelPageCountExp = pageWriteTrx.getUberPage().getPageCountExp(
				pageKind);

		// Remaining levels.
		for (int i = 0, l = levelPageCountExp.length; i < l; i++) {
			page = new IndirectPage();
			final IndirectPageLogKey logKey = new IndirectPageLogKey(pageKind, index,
					i, 0);
			reference.setLogKey(logKey);
			pageWriteTrx.putPageIntoCache(logKey, page);
			reference = page.getReference(0);
		}

		// Create new record page.
		final UnorderedKeyValuePage ndp = new UnorderedKeyValuePage(
				Fixed.ROOT_PAGE_KEY.getStandardProperty(), pageKind,
				Optional.<PageReference> empty(), pageWriteTrx);
		ndp.setDirty(true);
		reference.setKeyValuePageKey(0);
		reference.setLogKey(new IndirectPageLogKey(pageKind, index,
				levelPageCountExp.length, 0));

		// Create a {@link DocumentRootNode}.
		final Optional<SirixDeweyID> id = pageWriteTrx.getSession()
				.getResourceConfig().mDeweyIDsStored ? Optional.of(SirixDeweyID
				.newRootID()) : Optional.<SirixDeweyID> empty();
		final NodeDelegate nodeDel = new NodeDelegate(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, id);
		final StructNodeDelegate strucDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
		ndp.setEntry(0L, new DocumentRootNode(nodeDel, strucDel));
		pageWriteTrx.putPageIntoKeyValueCache(pageKind, 0, index,
				new RecordPageContainer<UnorderedKeyValuePage>(ndp, ndp));
	}

}
