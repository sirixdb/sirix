//package org.sirix.index.art.acc;
//
//import org.sirix.index.art.AbstractNavigableMapTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//import org.sirix.index.art.BinaryComparables;
//
//import java.util.NavigableMap;
//
//public class ARTLongTest extends AbstractNavigableMapTest<Long, Long> {
//	public ARTLongTest(String testName) {
//		super(testName);
//	}
//
//	@Override
//	public Long[] getSampleKeys() {
//		return new Long[] {
//				0xFFFFFF0000000000L, // stored as lazy leaf
//				0xFFFFFF0000000001L, // cause lazy leaf expansion, but compressed path size 7
//				0xFFFFFF0000000002L, // complete compressed path match FFFFFF00000000
//				0xFFFFFF0000000003L, // add two more with same 7 prefix bytes to cause growth to Node16
//				0xFFFFFF0000000004L,
//				0xFF00000000000000L, // incomplete compressed path match (branch out), update compressed path to FF
//				// and only FFFF00000000 for first 5 nodes
//				0xFF00000000000001L, // complete compressed path match FF
//				// deleting all previous would leave two children of FE and none with FF and hence
//				// cause updating only child of FF and refer directly to 000000000000, 000000000001 having CP as FE
//				0xFFFE000000000000L,
//				0xFFFE000000000001L,
//				// mix positives with negatives to test ordering
//				0x0000000000000000L,
//				0x0000000000000001L
//		};
//	}
//
//	@Override
//	public Long[] getSampleValues() {
//		return getSampleKeys();
//	}
//
//	@Override
//	public Long[] getOtherValues() {
//		return getOtherKeys();
//	}
//
//	@Override
//	public Long[] getNewSampleValues() {
//		return new Long[] {7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L, 15L, 16L, 17L};
//	}
//
//	@Override
//	public Long[] getOtherKeys() {
//		return new Long[] {2L, 3L, 4L, 5L, 6L};
//	}
//
//	@Override
//	public NavigableMap<Long, Long> makeObject() {
//		return new AdaptiveRadixTree<>(BinaryComparables.forLong());
//	}
//}
