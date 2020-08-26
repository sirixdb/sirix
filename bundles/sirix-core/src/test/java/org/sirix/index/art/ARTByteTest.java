package org.sirix.index.art;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ARTByteTest {
	@Test
	// TODO: replace with AbstractNavigableMapTest
	public void testInsertingAndDeletingAllInt8BitIntegers() throws ReflectiveOperationException {
		AdaptiveRadixTree<Byte, String> art = new AdaptiveRadixTree<>(BinaryComparables.forByte());

		// insert all
		byte i = Byte.MIN_VALUE;
		int expectedSize = 0;
		do {
			// floor test
			if (i != Byte.MIN_VALUE) {
				assertEquals(i - 1, (byte) art.floorKey(i));
			}
			else {
				assertNull(art.floorKey(i));
			}

			String value = String.valueOf(i);
			assertFalse(art.containsKey(i));
			assertFalse(art.containsValue(value));
			assertNull(art.put(i, value));
			expectedSize++;
			assertEquals(value, art.get(i));
			assertEquals(expectedSize, art.size());
			assertTrue(art.containsKey(i));
			assertTrue(art.containsValue(value));

			// lowerKey test
			if (i != Byte.MIN_VALUE) {
				assertEquals(i - 1, (byte) art.lowerKey(i));
			}
			else {
				assertNull(art.lowerKey(i));
			}
			i++;
		}
		while (i != Byte.MIN_VALUE);

		Map.Entry<Byte, String> firstEntry = art.firstEntry();
		assertEquals(String.valueOf(Byte.MIN_VALUE), firstEntry.getValue());
		assertEquals((Byte) Byte.MIN_VALUE, firstEntry.getKey());
		assertEquals((Byte) Byte.MIN_VALUE, art.firstKey());

		Map.Entry<Byte, String> lastEntry = art.lastEntry();
		assertEquals(String.valueOf(Byte.MAX_VALUE), lastEntry.getValue());
		assertEquals((Byte) Byte.MAX_VALUE, lastEntry.getKey());
		assertEquals((Byte) Byte.MAX_VALUE, art.lastKey());

		// assert parent of root is null
		Field root = art.getClass().getDeclaredField("root");
		root.setAccessible(true);
		assertNull(((Node) root.get(art)).parent());


		// test sorted order iteration
		i = Byte.MIN_VALUE;
		for (Map.Entry<Byte, String> entry : art.entrySet()) {
			assertEquals(i, (byte) entry.getKey());
			i++;
		}


		// remove one by one and check if others exist
		i = Byte.MIN_VALUE;
		do {

			// higherKey test
			if (i != Byte.MAX_VALUE) {
				try {
					assertEquals(i + 1, (byte) art.higherKey(i));
				}
				catch (NullPointerException e) {
					System.out.println(i);
					fail();
				}
			}
			else {
				assertNull(art.higherKey(i));
			}

			String value = String.valueOf(i);
			assertEquals(value, art.remove(i));
			expectedSize--;
			assertNull(art.get(i));
			assertEquals(expectedSize, art.size());

			// ceil test
			if (i != Byte.MAX_VALUE) {
				assertEquals(i + 1, (byte) art.ceilingKey(i));
			}
			else {
				assertNull(art.ceilingKey(i));
			}

			// others should exist
			for (byte j = ++i; j != Byte.MIN_VALUE; j++) {
				value = String.valueOf(j);
				assertEquals(value, art.get(j));
			}

		}
		while (i != Byte.MIN_VALUE);
	}
}
