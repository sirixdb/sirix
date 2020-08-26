//package org.sirix.index.art;
//
//import java.util.Iterator;
//
//import static org.junit.Assert.assertEquals;
//
///**
// *	When number of sample keys are very large, we need to avoid O(n^2) loops to keep the test runtime short.
// *	For example tests that call AbstractMapTest.verify() inside a loop.
// */
//public abstract class AbstractNavigableMapShortTest<K, V> extends AbstractNavigableMapTest<K, V> {
//	public AbstractNavigableMapShortTest(String testName) {
//		super(testName);
//	}
//
//	public void verifyShort() {
//		// verifyMap
//		int size = this.getConfirmed().size();
//		boolean empty = this.getConfirmed().isEmpty();
//		assertEquals("Map should be same size as HashMap", size, this.getMap().size());
//		assertEquals("Map should be empty if HashMap is", empty, this.getMap().isEmpty());
//
//		// verify entry set
//		assertEquals(size, this.entrySet.size());
//		assertEquals(empty, this.entrySet.isEmpty());
//
//		// verify key set
//		assertEquals(size, this.keySet.size());
//		assertEquals(empty, this.keySet.isEmpty());
//	}
//
//	@Override
//	public void verifyValues() {
//		// O(n^2)
//	}
//
//	@Override
//	public void testEntrySetRemoveAll() {
//		// involves creation of HashSet (same reason as testEntrySetRetainAll
//	}
//
//	@Override
//	public void testValuesIteratorRemoveChangesMap() {
//		// goes over all values O(n) using iterator
//		// and then does a contains check which makes it O(n^2)
//	}
//
//	@Override
//	public void testValuesRemoveChangesMap() {
//		// goes over all values O(n) then does a contains check which makes it O(n^2)
//	}
//
//	@Override
//	public void testEntrySetRetainAll() {
//		/*
//			creation of the HashSet is slow
//			at java.util.HashMap$TreeNode.find(java.base@12.0.2/HashMap.java:1921)
//			at java.util.HashMap$TreeNode.putTreeVal(java.base@12.0.2/HashMap.java:2040)
//			at java.util.HashMap.putVal(java.base@12.0.2/HashMap.java:633)
//			at java.util.HashMap.put(java.base@12.0.2/HashMap.java:607)
//			at java.util.HashSet.add(java.base@12.0.2/HashSet.java:220)
//			at java.util.AbstractCollection.addAll(java.base@12.0.2/AbstractCollection.java:352)
//			at java.util.HashSet.<init>(java.base@12.0.2/HashSet.java:120)
//			at org.apache.commons.collections4.map.AbstractMapTest.testEntrySetRetainAll(AbstractMapTest.java:1473)
//		*/
//	}
//
//	@Override
//	public void testMapContainsValue() {
//		/*
//			unless the unique value space is small
//			each containsValue is a O(n) operation
//		 */
//	}
//
//	/*
//		same as super, just skips key's duplicate check (since that'd be O(n^2))
//	 */
//	@Override
//	public void testSampleMappings() {
//		Object[] keys = this.getSampleKeys();
//		Object[] values = this.getSampleValues();
//		Object[] newValues = this.getNewSampleValues();
//		assertNotNull("failure in test: Must have keys returned from getSampleKeys.", keys);
//		assertNotNull("failure in test: Must have values returned from getSampleValues.", values);
//		assertEquals("failure in test: not the same number of sample keys and values.", keys.length, values.length);
//		assertEquals("failure in test: not the same number of values and new values.", values.length, newValues.length);
//
//		for (int i = 0; i < keys.length - 1; ++i) {
//			/*for(int j = i + 1; j < keys.length; ++j) {
//				assertTrue("failure in test: duplicate null keys.", keys[i] != null || keys[j] != null);
//				assertTrue("failure in test: duplicate non-null key.", keys[i] == null || keys[j] == null || !keys[i].equals(keys[j]) && !keys[j].equals(keys[i]));
//			}*/
//
//			assertTrue("failure in test: found null key, but isNullKeySupported is false.", keys[i] != null || this
//					.isAllowNullKey());
//			assertTrue("failure in test: found null value, but isNullValueSupported is false.", values[i] != null || this
//					.isAllowNullValue());
//			assertTrue("failure in test: found null new value, but isNullValueSupported is false.", newValues[i] != null || this
//					.isAllowNullValue());
//			assertTrue("failure in test: values should not be the same as new value", values[i] != newValues[i] && (values[i] == null || !values[i]
//					.equals(newValues[i])));
//		}
//
//	}
//
//	/*
//		same as super, but doesn't call verify inside put loop
//	 */
//	@Override
//	public void testMapPut() {
//		this.resetEmpty();
//		K[] keys = this.getSampleKeys();
//		V[] values = this.getSampleValues();
//		V[] newValues = this.getNewSampleValues();
//		int i;
//		if (this.isPutAddSupported()) {
//			Object o;
//			for (i = 0; i < keys.length; ++i) {
//				o = this.getMap().put(keys[i], values[i]);
//				this.getConfirmed().put(keys[i], values[i]);
//				// this.verify();
//				this.verifyShort();
//				assertTrue("First map.put should return null", o == null);
//				assertTrue("Map should contain key after put", this.getMap().containsKey(keys[i]));
//				// assertTrue("Map should contain value after put", this.getMap().containsValue(values[i]));
//			}
//
//			if (this.isPutChangeSupported()) {
//				for (i = 0; i < keys.length; ++i) {
//					o = this.getMap().put(keys[i], newValues[i]);
//					this.getConfirmed().put(keys[i], newValues[i]);
//					// this.verify();
//					this.verifyShort();
//					assertEquals("Map.put should return previous value when changed", values[i], o);
//					assertTrue("Map should still contain key after put when changed", this.getMap()
//							.containsKey(keys[i]));
//					//assertTrue("Map should contain new value after put when changed", this.getMap()
//					//		.containsValue(newValues[i]));
//					if (!this.isAllowDuplicateValues()) {
//						assertTrue("Map should not contain old value after put when changed", !this.getMap()
//								.containsValue(values[i]));
//					}
//				}
//			}
//			else {
//				try {
//					this.getMap().put(keys[0], newValues[0]);
//					fail("Expected IllegalArgumentException or UnsupportedOperationException on put (change)");
//				}
//				catch (IllegalArgumentException var12) {
//				}
//				catch (UnsupportedOperationException var13) {
//				}
//			}
//		}
//		else if (this.isPutChangeSupported()) {
//			this.resetEmpty();
//
//			try {
//				this.getMap().put(keys[0], values[0]);
//				fail("Expected UnsupportedOperationException or IllegalArgumentException on put (add) when fixed size");
//			}
//			catch (IllegalArgumentException var10) {
//			}
//			catch (UnsupportedOperationException var11) {
//			}
//
//			this.resetFull();
//			i = 0;
//
//			for (Iterator<K> it = this.getMap().keySet().iterator(); it.hasNext() && i < newValues.length; ++i) {
//				K key = it.next();
//				V o = this.getMap().put(key, newValues[i]);
//				V value = this.getConfirmed().put(key, newValues[i]);
//				// this.verify();
//				this.verifyShort();
//				assertEquals("Map.put should return previous value when changed", value, o);
//				assertTrue("Map should still contain key after put when changed", this.getMap().containsKey(key));
//				//assertTrue("Map should contain new value after put when changed", this.getMap()
//				//		.containsValue(newValues[i]));
//				if (!this.isAllowDuplicateValues()) {
//					assertTrue("Map should not contain old value after put when changed", !this.getMap()
//							.containsValue(values[i]));
//				}
//			}
//		}
//		else {
//			try {
//				this.getMap().put(keys[0], values[0]);
//				fail("Expected UnsupportedOperationException on put (add)");
//			}
//			catch (UnsupportedOperationException var9) {
//			}
//		}
//	}
//
//	/*
//		same as super, but doesn't call verify inside remove loop
//	 */
//	@Override
//	public void testMapRemove() {
//		if (!this.isRemoveSupported()) {
//			try {
//				this.resetFull();
//				this.getMap().remove(this.getMap().keySet().iterator().next());
//				fail("Expected UnsupportedOperationException on remove");
//			}
//			catch (UnsupportedOperationException var10) {
//			}
//
//		}
//		else {
//			this.resetEmpty();
//			Object[] keys = this.getSampleKeys();
//			Object[] values = this.getSampleValues();
//			Object[] other = keys;
//			int size = keys.length;
//
//			for (int var5 = 0; var5 < size; ++var5) {
//				Object key = other[var5];
//				Object o = this.getMap().remove(key);
//				assertTrue("First map.remove should return null", o == null);
//			}
//
//			this.verify();
//			this.resetFull();
//
//			for (int i = 0; i < keys.length; ++i) {
//				Object o = this.getMap().remove(keys[i]);
//				this.getConfirmed().remove(keys[i]);
//				// this.verify(); -- commented since it is an O(n) operation
//				this.verifyShort();
//				assertEquals("map.remove with valid key should return value", values[i], o);
//				assertNull(this.getMap().get(keys[i]));
//			}
//
//			other = this.getOtherKeys();
//			this.resetFull();
//			size = this.getMap().size();
//			Object[] var13 = other;
//			int var14 = other.length;
//
//			for (int var15 = 0; var15 < var14; ++var15) {
//				Object element = var13[var15];
//				Object o = this.getMap().remove(element);
//				assertNull("map.remove for nonexistent key should return null", o);
//				assertEquals("map.remove for nonexistent key should not shrink map", size, this.getMap().size());
//			}
//
//			this.verify();
//		}
//	}
//
//	@Override
//	public void testFloorKey() {
//		floorKey(true);
//	}
//
//	@Override
//	public void testFloorEntry() {
//		floorEntry(true);
//	}
//
//	@Override
//	public void testCeilingEntry() {
//		ceilingEntry(true);
//	}
//
//	@Override
//	public void testCeilingKey() {
//		ceilingKey(true);
//	}
//
//	@Override
//	public void testPollFirstEntry() {
//		pollFirstEntry(true);
//	}
//
//	@Override
//	public void testPollLastEntry() {
//		pollLastEntry(true);
//	}
//}
