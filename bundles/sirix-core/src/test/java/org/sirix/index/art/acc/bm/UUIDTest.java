//package org.sirix.index.art.acc.bm;
//
//import org.sirix.index.art.AbstractNavigableMapShortTest;
//import org.sirix.index.art.AdaptiveRadixTree;
//import org.sirix.index.art.BinaryComparables;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.*;
//
//public class UUIDTest extends AbstractNavigableMapShortTest<String, String> {
//
//	private static final Path RESOURCES = Path.of("src", "test", "resources", "art-index");
//
//	private static final String[] sampleKeys = loadUUIDs();
//	private static final String[] newSampleValues = reverseSampleKeys();
//	private static final String[] otherKeys = generateOtherKeys();
//
//	public UUIDTest(String testName) {
//		super(testName);
//	}
//
//	private static String[] generateOtherKeys() {
//		int n = 10;
//		String[] otherKeys = new String[n];
//		for (int i = 0; i < 10; i++) {
//			otherKeys[i] = UUID.randomUUID().toString();
//		}
//		return otherKeys;
//	}
//
//	private static String[] reverseSampleKeys() {
//		String[] keys = sampleKeys.clone();
//		final List<String> keysAsList = Arrays.asList(keys);
//		Collections.reverse(keysAsList);
//		return keysAsList.toArray(new String[] {});
//	}
//
//	private static String[] loadUUIDs() {
//		try {
//			List<String> lines = Files.readAllLines(RESOURCES.resolve("uuid.txt"), StandardCharsets.UTF_8);
//			return lines.toArray(new String[0]);
//		}
//		catch (IOException e) {
//			throw new RuntimeException("failed to load uuids", e);
//		}
//	}
//
//	@Override
//	public String[] getSampleKeys() {
//		return sampleKeys;
//	}
//
//	@Override
//	public String[] getSampleValues() {
//		return sampleKeys;
//	}
//
//	@Override
//	public String[] getNewSampleValues() {
//		return newSampleValues;
//	}
//
//	@Override
//	public String[] getOtherKeys() {
//		return otherKeys;
//	}
//
//	@Override
//	public String[] getOtherValues() {
//		return otherKeys;
//	}
//
//	@Override
//	public NavigableMap<String, String> makeObject() {
//		return new AdaptiveRadixTree<>(BinaryComparables.forString());
//	}
//}
