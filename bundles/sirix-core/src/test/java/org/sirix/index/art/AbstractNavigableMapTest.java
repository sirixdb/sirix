//package org.sirix.index.art;
//
//import org.apache.commons.collections4.BulkTest;
//import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
//import org.apache.commons.collections4.map.AbstractSortedMapTest;
//
//import java.util.*;
//
//public abstract class AbstractNavigableMapTest<K, V> extends AbstractSortedMapTest<K, V> {
//	public AbstractNavigableMapTest(String testName) {
//		super(testName);
//	}
//
//	public NavigableMap<K, V> makeFullMap() {
//		return (NavigableMap<K, V>) super.makeFullMap();
//	}
//
//	@Override
//	public abstract NavigableMap<K, V> makeObject();
//
//	@Override
//	public NavigableMap<K, V> getMap() {
//		return (NavigableMap<K, V>) super.getMap();
//	}
//
//	@Override
//	public NavigableMap<K, V> getConfirmed() {
//		return (NavigableMap<K, V>) super.getConfirmed();
//	}
//
//	@Override
//	public void verify() {
//		super.verify();
//		// just as org.apache.commons.collections4.set.AbstractNavigableSetTest
//		Set<Map.Entry<K, V>> entrySet = this.getMap().entrySet();
//		for (Map.Entry<K, V> entry : entrySet) {
//			assertEquals(this.getConfirmed().higherEntry(entry.getKey()),
//					this.getMap().higherEntry(entry.getKey()));
//			assertEquals(this.getConfirmed().higherKey(entry.getKey()),
//					this.getMap().higherKey(entry.getKey()));
//			assertEquals(this.getConfirmed().lowerEntry(entry.getKey()),
//					this.getMap().lowerEntry(entry.getKey()));
//			assertEquals(this.getConfirmed().lowerKey(entry.getKey()),
//					this.getMap().lowerKey(entry.getKey()));
//			assertEquals(this.getConfirmed().lowerKey(entry.getKey()),
//					this.getMap().lowerKey(entry.getKey()));
//			assertEquals(this.getConfirmed().ceilingEntry(entry.getKey()),
//					this.getMap().ceilingEntry(entry.getKey()));
//			assertEquals(this.getConfirmed().ceilingKey(entry.getKey()),
//					this.getMap().ceilingKey(entry.getKey()));
//			assertEquals(this.getConfirmed().floorEntry(entry.getKey()),
//					this.getMap().floorEntry(entry.getKey()));
//			assertEquals(this.getConfirmed().floorKey(entry.getKey()),
//					this.getMap().floorKey(entry.getKey()));
//		}
//	}
//
//	@Override
//	public void testSampleMappings() {
//		super.testSampleMappings();
//
//		// TODO: push this upstream (Apache Common's test suite)
//		// no common keys in "other keys" and "sample keys"
//		// map.remove test assumes this
//		K[] sampleKeys = this.getSampleKeys();
//		for (K otherKey : this.getOtherKeys()) {
//			for (K sampleKey : sampleKeys) {
//				if (otherKey.equals(sampleKey)) {
//					fail("there should be no common keys in otherKeys and sampleKeys,"
//							+ " but key " + sampleKey + " found to be common.");
//				}
//			}
//		}
//	}
//
//	public NavigableMap<K, V> makeConfirmedMap() {
//		return new TreeMap<>();
//	}
//
//	// if provided object to entrySet is not an "entry" then we always return false
//	public void testEntrySetContainsForNonEntryObject() {
//		resetFull();
//		Object ob = new Object();
//		assertEquals(this.getConfirmed().entrySet().contains(ob), this.getMap().entrySet().contains(ob));
//	}
//
//	public void testMapGetNullKey() {
//		this.resetFull();
//		// no need to test if isAllowNullKey is true
//		// because get tests would test for all sample keys
//		// which would include getting a null key
//		if (!this.isAllowNullKey()) {
//			try {
//				this.getMap().get(null);
//				fail("get(null) should throw NPE/IAE");
//			}
//			catch (NullPointerException var3) {
//			}
//			catch (IllegalArgumentException var4) {
//			}
//		}
//	}
//
//	public void testHigherEntry() {
//		assertNull(this.makeObject().higherEntry(this.getSampleKeys()[0]));
//		resetFull();
//		for (K key : this.getOtherKeys()) {
//			assertEquals(this.getConfirmed().higherEntry(key), this.getMap().higherEntry(key));
//			assertEquals(this.getConfirmed().higherKey(key), this.getMap().higherKey(key));
//		}
//	}
//
//	public void testFirstKeyOnEmptyMap() {
//		NavigableMap<K, V> nm = this.makeObject();
//		try {
//			nm.firstKey();
//		}
//		catch (NoSuchElementException e) {
//		}
//	}
//
//	public void testFirstEntry() {
//		assertNull(this.makeObject().firstEntry());
//		NavigableMap<K, V> nm = this.makeFullMap();
//		assertEquals(nm.entrySet().iterator().next(), nm.firstEntry());
//	}
//
//	public void testLastEntry() {
//		assertNull(this.makeObject().lastEntry());
//		NavigableMap<K, V> nm = this.makeFullMap();
//		Map.Entry<K, V> last = null;
//
//		for (Map.Entry<K, V> kvEntry : nm.entrySet()) {
//			last = kvEntry;
//		}
//
//		assertEquals(last, nm.lastEntry());
//	}
//
//	protected void pollFirstEntry(boolean shortTest) {
//		assertNull(this.makeObject().pollFirstEntry());
//		resetFull();
//		while (!this.getMap().isEmpty()) {
//			assertEquals(this.getConfirmed().pollFirstEntry(), this.getMap().pollFirstEntry());
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	protected void pollLastEntry(boolean shortTest) {
//		assertNull(this.makeObject().pollLastEntry());
//		resetFull();
//		while (!this.getMap().isEmpty()) {
//			assertEquals(this.getConfirmed().pollLastEntry(), this.getMap().pollLastEntry());
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	public void testPollFirstEntry() {
//		pollFirstEntry(false);
//	}
//
//
//	public void testPollLastEntry() {
//		pollLastEntry(false);
//	}
//
//	protected void ceilingEntry(boolean shortTest) {
//		assertNull(this.makeObject().ceilingEntry(this.getSampleKeys()[0]));
//		resetFull();
//		K[] keys = this.getSampleKeys();
//		V[] values = this.getSampleValues();
//		for (int i = 0; i < this.getMap().size(); i++) {
//			assertEquals(this.getMap().remove(keys[i]), this.getConfirmed().remove(keys[i]));
//			assertEquals(this.getMap().ceilingEntry(keys[i]),
//					this.getConfirmed().ceilingEntry(keys[i]));
//			if (!shortTest) {
//				verify();
//			}
//			this.getMap().put(keys[i], values[i]);
//			this.getConfirmed().put(keys[i], values[i]);
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	public void testCeilingEntry() {
//		ceilingEntry(false);
//	}
//
//	protected void ceilingKey(boolean shortTest) {
//		assertNull(this.makeObject().ceilingKey(this.getSampleKeys()[0]));
//		resetFull();
//		K[] keys = this.getSampleKeys();
//		V[] values = this.getSampleValues();
//		for (int i = 0; i < this.getMap().size(); i++) {
//			assertEquals(this.getMap().remove(keys[i]), this.getConfirmed().remove(keys[i]));
//			assertEquals(this.getMap().ceilingKey(keys[i]),
//					this.getConfirmed().ceilingKey(keys[i]));
//			if (!shortTest) {
//				verify();
//			}
//			this.getMap().put(keys[i], values[i]);
//			this.getConfirmed().put(keys[i], values[i]);
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	public void testCeilingKey() {
//		ceilingKey(false);
//	}
//
//	protected void floorEntry(boolean shortTest) {
//		assertNull(this.makeObject().floorEntry(this.getSampleKeys()[0]));
//		resetFull();
//		K[] keys = this.getSampleKeys();
//		V[] values = this.getSampleValues();
//		for (int i = 0; i < this.getMap().size(); i++) {
//			assertEquals(this.getConfirmed().remove(keys[i]), this.getMap().remove(keys[i]));
//			assertEquals(this.getMap().floorEntry(keys[i]),
//					this.getConfirmed().floorEntry(keys[i]));
//			if (!shortTest) {
//				verify();
//			}
//			this.getMap().put(keys[i], values[i]);
//			this.getConfirmed().put(keys[i], values[i]);
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	public void testFloorEntry() {
//		floorEntry(false);
//	}
//
//	protected void floorKey(boolean shortTest) {
//		assertNull(this.makeObject().floorKey(this.getSampleKeys()[0]));
//		resetFull();
//		K[] keys = this.getSampleKeys();
//		V[] values = this.getSampleValues();
//		for (int i = 0; i < this.getMap().size(); i++) {
//			assertEquals(this.getMap().remove(keys[i]), this.getConfirmed().remove(keys[i]));
//			assertEquals(this
//							.getMap().floorKey(keys[i]),
//					this.getConfirmed().floorKey(keys[i]));
//			if (!shortTest) {
//				verify();
//			}
//			this.getMap().put(keys[i], values[i]);
//			this.getConfirmed().put(keys[i], values[i]);
//			if (!shortTest) {
//				verify();
//			}
//		}
//		verify();
//	}
//
//	public void testFloorKey() {
//		floorKey(false);
//	}
//
//	/*
//		copy of Entry is needed because Map.Entry is undefined after the map is modified.
//		(see Javadoc of Map.Entry)
//		https://stackoverflow.com/questions/45863470/treemap-iterator-remove-modifies-the-last-entry
//	 */
//	private Map.Entry<K, V> removeIth(Map<K, V> m, int pos) {
//		Iterator<Map.Entry<K, V>> it = m.entrySet().iterator();
//		Map.Entry<K, V> toRemove = null;
//		for (int j = 0; j <= pos; j++) {
//			toRemove = it.next();
//		}
//		DefaultMapEntry<K, V> removed = new DefaultMapEntry<>(toRemove);
//		it.remove();
//		return removed;
//	}
//
//	public void testSameDescendingMap() {
//		NavigableMap<K, V> m = this.makeObject();
//		assertSame(m.descendingMap(), m.descendingMap());
//	}
//
//	public void testSameKeySet() {
//		NavigableMap<K, V> m = this.makeObject();
//		assertSame(m.navigableKeySet(), m.navigableKeySet());
//	}
//
//	// should be same as NavigableMap's comparator
//	public void testKeySetComparator() {
//		NavigableMap<K, V> m = this.makeObject();
//		assertSame(m.comparator(), m.navigableKeySet().comparator());
//	}
//
//	public BulkTest bulkTestDescendingMap() {
//		return new TestDescendingMap<>(this);
//	}
//
//	public BulkTest bulkTestNavigableKeySet() {
//		return new TestNavigableKeySet<>(this, true);
//	}
//
//	public BulkTest bulkTestDescendingKeySet() {
//		return new TestNavigableKeySet<>(this, false);
//	}
//
//	public static class TestNavigableKeySet<K, V> extends AbstractNavigableSetTest<K> {
//		private final AbstractNavigableMapTest<K, V> main;
//		private final boolean asc;
//
//		public TestNavigableKeySet(AbstractNavigableMapTest<K, V> main, boolean asc) {
//			super("TestNavigableKeySet");
//			this.main = main;
//			this.asc = asc;
//		}
//
//		// although the view does not support element add,
//		// the map could be pre filled and then we test
//		// over it's view
//		@Override
//		public NavigableSet<K> makeFullCollection() {
//			NavigableMap<K, V> map = this.main.makeObject();
//			K[] elements = getFullElements();
//			for (K element : elements) {
//				// all same values, doesn't matter since we're testing a "KeySet"
//				map.put(element, this.main.getSampleValues()[0]);
//			}
//			return asc ? map.navigableKeySet() : map.descendingKeySet();
//		}
//
//		// false since it is just a view
//		@Override
//		public boolean isAddSupported() {
//			return false;
//		}
//
//		// since ART doesn't support null keys, the key set also cannot
//		@Override
//		public boolean isNullSupported() {
//			return false;
//		}
//
//		@Override
//		public NavigableSet<K> makeObject() {
//			return asc ? this.main.makeObject().navigableKeySet() :
//					this.main.makeObject().descendingKeySet();
//		}
//
//		@Override
//		public NavigableSet<K> makeConfirmedCollection() {
//			return asc ? new TreeSet<>() : new TreeSet<K>().descendingSet();
//		}
//
//		public K[] order(K[] elements) {
//			if (asc) {
//				Arrays.sort(elements);
//			}
//			else {
//				Arrays.sort(elements, Collections.reverseOrder());
//			}
//			return elements;
//		}
//	}
//
//	public static class TestDescendingMap<K, V> extends AbstractNavigableMapTest<K, V> {
//		private final AbstractNavigableMapTest<K, V> main;
//
//		public TestDescendingMap(AbstractNavigableMapTest<K, V> main) {
//			super("TestDescendingMap");
//			this.main = main;
//		}
//
//		// need to override since TestDescendingMap runs all tests over
//		// vanilla AbstractNavigableMapTest.
//		// Vanilla AbstractNavigableTest's keySet test
//		// is creating integer keys by default
//		// but we want to delegate to whatever main's version is.
//		// In general it would be best to override all tests and call main's version
//		// use composition here rather than inheritance.
//		// have a ForwardingAbstractNavigableTest
//		// that calls all tests on passed in AbstractNavigableMapTest
//		@Override
//		public BulkTest bulkTestNavigableKeySet() {
//			return this.main.bulkTestNavigableKeySet();
//		}
//
//		@Override
//		public BulkTest bulkTestDescendingKeySet() {
//			return this.main.bulkTestDescendingKeySet();
//		}
//
//		public void resetFull() {
//			this.main.resetFull();
//			super.resetFull();
//		}
//
//		public void verify() {
//			super.verify();
//			this.main.verify();
//		}
//
//		@Override
//		public void resetEmpty() {
//			this.main.resetEmpty();
//			super.resetEmpty();
//		}
//
//		@Override
//		public K[] getSampleKeys() {
//			return this.main.getSampleKeys();
//		}
//
//		@Override
//		public NavigableMap<K, V> makeObject() {
//			return this.main.makeObject().descendingMap();
//		}
//
//		@Override
//		public NavigableMap<K, V> makeFullMap() {
//			return this.main.makeFullMap().descendingMap();
//		}
//
//		@Override
//		public NavigableMap<K, V> makeConfirmedMap() {
//			return this.main.makeConfirmedMap().descendingMap();
//		}
//
//		@Override
//		public BulkTest bulkTestDescendingMap() {
//			return null;
//		}
//
//		@Override
//		public BulkTest bulkTestHeadMap() {
//			return new TestHeadMap<>(this);
//		}
//
//		@Override
//		public BulkTest bulkTestTailMap() {
//			return new TestTailMap<>(this);
//		}
//
//		@Override
//		public BulkTest bulkTestSubMap() {
//			return new TestSubMap<>(this);
//		}
//
//		// TODO: explain the need (TestHeadMap doesn't override makeConfirmedMap to main's impl)
//		public static class TestHeadMap<K, V> extends AbstractSortedMapTest.TestHeadMap<K, V> {
//			private final AbstractNavigableMapTest<K, V> main;
//
//			public TestHeadMap(AbstractNavigableMapTest<K, V> main) {
//				super(main);
//				this.main = main;
//			}
//
//			@Override
//			public NavigableMap<K, V> makeConfirmedMap() {
//				return this.main.makeConfirmedMap();
//			}
//		}
//
//		public static class TestSubMap<K, V> extends AbstractSortedMapTest.TestSubMap<K, V> {
//			private final AbstractNavigableMapTest<K, V> main;
//
//			public TestSubMap(AbstractNavigableMapTest<K, V> main) {
//				super(main);
//				this.main = main;
//			}
//
//			@Override
//			public NavigableMap<K, V> makeConfirmedMap() {
//				return this.main.makeConfirmedMap();
//			}
//		}
//
//		public static class TestTailMap<K, V> extends AbstractSortedMapTest.TestTailMap<K, V> {
//			private final AbstractNavigableMapTest<K, V> main;
//
//			public TestTailMap(AbstractNavigableMapTest<K, V> main) {
//				super(main);
//				this.main = main;
//			}
//
//			@Override
//			public NavigableMap<K, V> makeConfirmedMap() {
//				return this.main.makeConfirmedMap();
//			}
//		}
//	}
//
//}
