//package org.sirix.index.art.acc;
//
//import org.sirix.index.art.AbstractNavigableMapTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//import org.sirix.index.art.BinaryComparables;
//
//import java.util.NavigableMap;
//
//public class ARTIntegerTest extends AbstractNavigableMapTest<Integer, Integer> {
//
//	public ARTIntegerTest(String testName) {
//		super(testName);
//	}
//
//	@Override
//	public Integer[] getSampleKeys() {
//		return new Integer[] {
//				0xFFFFFF00, // stored as lazy leaf
//				0xFFFFFF01, // cause lazy leaf expansion, but compressed path size 3
//				0xFFFFFF02, // complete compressed path match FFFFFF
//				0xFFFFFF03, // add two more with same 3 prefix bytes to cause growth to Node16
//				0xFFFFFF04,
//				0xFF000000, // incomplete compressed path match (branch out), update compressed path to FF
//				// and only FF for first 5 nodes
//				0xFF000001, // complete compressed path match FF
//				// deleting all previous would leave two children of FE and none with FF and hence
//				// cause updating only child of FF and refer directly to 00, 01 having CP as FE
//				0xFFFE0000,
//				0xFFFE0001,
//				// mix positives with negatives to test ordering
//				0x00000000,
//				0x00000001
//		};
//	}
//
//	@Override
//	public Integer[] getSampleValues() {
//		return getSampleKeys();
//	}
//
//	@Override
//	public Integer[] getOtherValues() {
//		return getOtherKeys();
//	}
//
//	@Override
//	public Integer[] getNewSampleValues() {
//		return new Integer[] {7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17};
//	}
//
//	@Override
//	public Integer[] getOtherKeys() {
//		return new Integer[] {2, 3, 4, 5, 6};
//	}
//
//	@Override
//	public NavigableMap<Integer, Integer> makeObject() {
//		return new AdaptiveRadixTree<>(BinaryComparables.forInteger());
//	}
//}
