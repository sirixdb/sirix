//package org.sirix.index.art.acc;
//
//import org.sirix.index.art.AbstractNavigableMapShortTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//import org.sirix.index.art.BinaryComparables;
//
//import java.util.*;
//
//public class ARTShortTest extends AbstractNavigableMapShortTest<Short, String> {
//	private static final int NUMBER_OF_OTHERS = 5;
//	private static final int NUMBER_OF_SHORTS = 1 << 16;
//	private static final String[] otherValues = createSampleValues(NUMBER_OF_OTHERS);
//	private static final String[] sampleValues = createSampleValues(NUMBER_OF_SHORTS - NUMBER_OF_OTHERS);
//	private static final String[] newValues = createSampleValues(NUMBER_OF_SHORTS - NUMBER_OF_OTHERS);
//
//	public ARTShortTest(String testName) {
//		super(testName);
//	}
//
//	@Override
//	public Short[] getSampleKeys() {
//		List<Short> l = new ArrayList<>(NUMBER_OF_SHORTS - NUMBER_OF_OTHERS);
//		List<Short> others = Arrays.asList(getOtherKeys());
//		short i = Short.MIN_VALUE;
//		do {
//			if (!others.contains(i)) {
//				l.add(i);
//			}
//			i++;
//		}
//		while (i != Short.MIN_VALUE);
//		return l.toArray(new Short[0]);
//	}
//
//	private static String[] createSampleValues(int n) {
//		String[] s = new String[n];
//		for (int i = 0; i < s.length; i++) {
//			s[i] = UUID.randomUUID().toString();
//		}
//		return s;
//	}
//
//	@Override
//	public String[] getSampleValues() {
//		return sampleValues;
//	}
//
//	@Override
//	public String[] getOtherValues() {
//		return otherValues;
//	}
//
//	@Override
//	public String[] getNewSampleValues() {
//		return newValues;
//	}
//
//	@Override
//	public Short[] getOtherKeys() {
//		return new Short[] {2, 3, 4, 5, 6};
//	}
//
//	@Override
//	public NavigableMap<Short, String> makeObject() {
//		return new AdaptiveRadixTree<>(BinaryComparables.forShort());
//	}
//
//}
