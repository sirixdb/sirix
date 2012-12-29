package org.sirix.xquery.node;

import javax.annotation.Nonnull;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.XMarkTest;
import org.brackit.xquery.XQuery;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Store;
import org.junit.After;
import org.sirix.xquery.SirixCompileChain;

/**
 * XMark test.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class SirixXMarkTest extends XMarkTest {

	/** Sirix database store. */
	private DBStore mStore;

	@Override
	protected Store createStore() throws Exception {
		mStore = new DBStore();
		return mStore;
	}

	@Override
	protected XQuery xquery(final @Nonnull String query) throws QueryException {
		return new XQuery(new SirixCompileChain(mStore), query);
	}

	@Override
	protected Collection<?> createDoc(final @Nonnull DocumentParser parser)
			throws DocumentException {
		mStore.create("testCollection", parser);
		return mStore.lookup("testCollection");
	}

	@After
	public void commit() throws DocumentException {
		mStore.commitAll();
		mStore.close();
	}
}
