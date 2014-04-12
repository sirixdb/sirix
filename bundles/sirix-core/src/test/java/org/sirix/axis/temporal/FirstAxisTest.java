package org.sirix.axis.temporal;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.NodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.utils.DocumentCreater;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link FirstAxis}.
 * 
 * @author Johannes Lichtenberger
 *
 */
public final class FirstAxisTest {

	/** Number of iterations. */
	private static final int ITERATIONS = 5;

	/** The {@link Holder} instance. */
	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		DocumentCreater.createVersioned(Holder.generateWtx().getWtx());
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testAxis() throws SirixException {
		final NodeReadTrx firstRtx = holder.getSession().beginNodeReadTrx(1);

		new IteratorTester<NodeReadTrx>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
				ImmutableList.of(firstRtx), null) {
			{
				ignoreSunJavaBug6529795();
			}

			@Override
			protected Iterator<NodeReadTrx> newTargetIterator() {
				return new FirstAxis(holder.getRtx());
			}
		}.test();
	}
}
