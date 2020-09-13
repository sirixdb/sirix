//package org.sirix.index.art.acc;
//
//import org.sirix.index.art.AbstractNavigableMapTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//import org.sirix.index.art.BinaryComparables;
//import org.sirix.index.art.NavigableKeySetStringTest;
//import junit.framework.Test;
//import org.apache.commons.collections4.BulkTest;
//
//import java.util.NavigableMap;
//
//public class ARTStringTest extends AbstractNavigableMapTest<String, String> {
//
//	public ARTStringTest(String testName) {
//		super(testName);
//	}
//
//	public static Test suite() {
//		return BulkTest.makeSuite(ARTStringTest.class);
//	}
//
//	@Override
//	public NavigableMap<String, String> makeObject() {
//		return new AdaptiveRadixTree<>(BinaryComparables.forString());
//	}
//
//	/*
//	 	CLEANUP:
//	 	changing sample keys to introduce baaar, baaaz, baoz
//		which cause branchOut (since lcp is not totally equal)
//
//	 	changing sample keys to introduce fooooooooz, fooooooood, fooooooooe
//	 	which cause optimistic path compression jump
//
//	 	the "fooooooooee" is used to prefix with fooooooooe and cause
//	 	updating of compressed path of only child when removing fooooooooe
//	 	(since fooooooooz, fooooooood would've been removed already when removing fooooooooe)
//
//	 	but better to write out a separate test that brings this out behaviour
//
//	 	we also insert key, key2 (where key2 is prefix of key)
//	 */
//	@Override
//	public String[] getSampleKeys() {
//		Object[] result = new String[] {"fooooooooz", "fooooooood", "fooooooooe", "fooooooooee", "baaar", "baaaz", "tmp", "baoz", "hello", "goodbye", "we'll", "see", "you", "all", "", "key", "key2", "nonnullkey"};
//		return (String[]) result;
//	}
//
//	// For replaced by foooe to cause higherKey to match against compressed path of "fooooooooz",
//	// "fooooooood", "fooooooooe" and then be less than compressed path and hence get first on the level.
//	// ideally we should include a separate test with such set special set of keys
//	// inducing the behaviour.
//	// for now we change the sample keys
//	@Override
//	public Object[] getOtherNonNullStringElements() {
//		return new Object[] {"foooe", "then", "despite", "space", "I", "would", "be", "brought", "From", "limits", "far", "remote", "where", "thou", "dost", "stay"};
//	}
//
//	// since default sample keys in AbstractNavigableSet are integers
//	@Override
//	public BulkTest bulkTestNavigableKeySet() {
//		return new NavigableKeySetStringTest(this, true);
//	}
//
//	@Override
//	public BulkTest bulkTestDescendingKeySet() {
//		return new NavigableKeySetStringTest(this, false);
//	}
//}
