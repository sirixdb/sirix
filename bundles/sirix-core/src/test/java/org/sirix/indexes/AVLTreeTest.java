package org.sirix.indexes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.access.Movement;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.index.SearchMode;
import org.sirix.index.value.AVLTree;
import org.sirix.index.value.References;
import org.sirix.index.value.Value;
import org.sirix.index.value.ValueKind;
import org.sirix.settings.Constants;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Test the AVLTree implementation.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public class AVLTreeTest {

	/** {@link Holder} reference. */
	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		holder = Holder.generateSession();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testAttributeIndex() throws SirixException {
		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		wtx.insertElementAsFirstChild(new QName("bla"));
		wtx.insertAttribute(new QName("foo"), "bar", Movement.TOPARENT);
		wtx.insertAttribute(new QName("foobar"), "baz", Movement.TOPARENT);
		wtx.insertElementAsFirstChild(new QName("blabla"));
		wtx.insertAttribute(new QName("foo"), "bar", Movement.TOPARENT);
		wtx.insertAttribute(new QName("foobar"), "baz", Movement.TOPARENT);
		wtx.commit();
		final AVLTree<Value, References> attIndex = wtx
				.getAttributeValueIndex();
		final Optional<References> fooRefs = attIndex.get(
				new Value("foo".getBytes(Constants.DEFAULT_ENCODING), 0,
						ValueKind.ATTRIBUTE), SearchMode.EQUAL);
		assertTrue(!fooRefs.isPresent());
		final Optional<References> barRefs1 = attIndex.get(
				new Value("bar".getBytes(Constants.DEFAULT_ENCODING), 2,
						ValueKind.ATTRIBUTE), SearchMode.EQUAL);
		check(barRefs1, ImmutableSet.of(2L));
		final Optional<References> barRefs2 = attIndex.get(
				new Value("bar".getBytes(Constants.DEFAULT_ENCODING), 5,
						ValueKind.ATTRIBUTE), SearchMode.EQUAL);
		check(barRefs2, ImmutableSet.of(5L));
	}

	private void check(final @Nonnull Optional<References> barRefs,
			final @Nonnull Set<Long> keys) {
		assertTrue(barRefs.isPresent());
		assertEquals(keys, barRefs.get().getNodeKeys());
	}

	@Test
	public void testTextIndex() throws SirixException {
		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		wtx.insertElementAsFirstChild(new QName("bla"));
		wtx.insertTextAsFirstChild("bla");
		wtx.insertElementAsRightSibling(new QName("blabla"));
		wtx.insertTextAsFirstChild("blabla");
		wtx.commit();
		final AVLTree<Value, References> textIndex = wtx
				.getTextValueIndex();
	}

}
