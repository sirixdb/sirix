package org.sirix.xquery.node;

import javax.annotation.Nonnull;

import org.brackit.xquery.XMarkTest;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.junit.After;

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
	protected Collection<?> createDoc(final @Nonnull DocumentParser parser)
			throws DocumentException {
		mStore = new DBStore();
		mStore.create("testCollection", parser);
		return mStore.lookup("testCollection");
	}

	@After
	public void commit() throws DocumentException {
		mStore.commitAll();
		mStore.close();
	}
}
