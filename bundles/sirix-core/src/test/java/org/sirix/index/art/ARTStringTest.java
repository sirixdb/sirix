package org.sirix.index.art;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ARTStringTest {
	private static final String BAA = "BAA";
	private static final String BAR = "BAR";
	private static final String BAZ = "BAZ";
	private static final String BOZ = "BOZ";
	private static final String BARCA = "BARCA";
	private static final String BARK = "BARK";


	@Test
	public void testSharedPrefixRemove_onlyChildLeaf() {
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(BinaryComparables.forString());

		assertNull(art.put(BAA, "0"));
		assertNull(art.put(BAR, "1"));
		assertNull(art.put(BAZ, "2"));
		assertNull(art.put(BOZ, "3"));
		assertEquals("0", art.get(BAA));
		assertEquals("1", art.get(BAR));
		assertEquals("2", art.get(BAZ));
		assertEquals("3", art.get(BOZ));

		// remove BAR that shares prefix A with BAZ
		assertEquals("1", art.remove(BAR));

		// path to BAZ should still exist
		assertEquals("2", art.get(BAZ));

		// untouched
		assertEquals("3", art.get(BOZ));
	}


	@Test
	public void testSharedPrefixRemove_onlyChildInnerNode() {
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(BinaryComparables.forString());

		assertNull(art.put(BARCA, "1"));
		assertNull(art.put(BAZ, "2"));
		assertNull(art.put(BOZ, "3"));
		assertNull(art.put(BARK, "4"));
		assertEquals("1", art.get(BARCA));
		assertEquals("2", art.get(BAZ));
		assertEquals("3", art.get(BOZ));
		assertEquals("4", art.get(BARK));

		/*
              p = B
		take O	/      \  take A
       leaf BOZ    inner
          /          \ p = R
      leaf BAZ      inner
			   	         /     \
				    leaf BARK   leaf BARCA
		 */


		// remove BAZ that shares prefix BA with node parent of BARCA, BARK
		assertEquals("2", art.remove(BAZ));

		/*
        after removing BAZ

           		p = B
		take O	/      \  take A
			leaf BOZ   inner p = R
	   	   /           \
		 leaf BARK      leaf BARCA
		 */

		// path to BARCA and BARK should still exist
		assertEquals("4", art.get(BARK));
		assertEquals("1", art.get(BARCA));

		// untouched
		assertEquals("3", art.get(BOZ));
	}


	/*
		should cause initial lazy stored leaf to split and have "BA" path compressed
	 */
	@Test
	public void testSharedPrefixInsert() {
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(BinaryComparables.forString());

		assertNull(art.put(BAR, "1"));
		assertNull(art.put(BAZ, "2"));
		assertEquals("1", art.get(BAR));
		assertEquals("2", art.get(BAZ));
	}

	@Test
	public void testBreakCompressedPath() {
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(BinaryComparables.forString());

		assertNull(art.put(BAR, "1"));
		assertNull(art.put(BAZ, "2"));
		assertNull(art.put(BOZ, "3")); // breaks compressed path of BAR, BAZ
		assertEquals("1", art.get(BAR));
		assertEquals("2", art.get(BAZ));
		assertEquals("3", art.get(BOZ));
	}

	@Test
	public void testPrefixesInsert() {
		AdaptiveRadixTree<String, String> art = new AdaptiveRadixTree<>(BinaryComparables.forString());

		assertNull(art.put(BAR, "1"));
		assertEquals("1", art.get(BAR));

		assertNull(art.put(BARCA, "2"));
		assertEquals("2", art.get(BARCA));
	}
}
