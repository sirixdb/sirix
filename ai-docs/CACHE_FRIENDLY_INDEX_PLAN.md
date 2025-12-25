# Cache-Friendly Secondary Index Structure Plan

## Executive Summary

The current secondary index implementation uses Red-Black Trees (RBTree) stored as individual node records within `KeyValueLeafPage`s in a trie structure. This design has significant cache-friendliness issues:

1. **Pointer-chasing**: Each tree node stores only key references (`leftChildKey`, `rightChildKey`), requiring separate page lookups per traversal step
2. **Poor locality**: Related keys can be scattered across different `KeyValueLeafPage`s
3. **Random I/O**: Tree traversal pattern leads to unpredictable page access

This document proposes **three alternative approaches** with detailed implementation plans.

---

## Current Architecture Analysis

### How Secondary Indexes Work Today

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INDEX PAGE STRUCTURE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   PathPage / CASPage / NamePage                                            │
│      ├── IndirectPage (trie level 0)                                       │
│      │      ├── IndirectPage (trie level 1)                                │
│      │      │      └── KeyValueLeafPage [1024 slots]                       │
│      │      │             ├── slot[0]: RBNodeKey (nodeKey=1)               │
│      │      │             ├── slot[1]: RBNodeValue (nodeKey=1 value)       │
│      │      │             ├── slot[2]: RBNodeKey (nodeKey=2)               │
│      │      │             ├── slot[3]: RBNodeValue (nodeKey=2 value)       │
│      │      │             └── ...                                          │
│      │      └── KeyValueLeafPage [1024 slots]                              │
│      │             └── More RBTree nodes...                                │
│      └── ...                                                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

### RBNodeKey Structure (Current)
```java
public final class RBNodeKey<K extends Comparable<? super K>> {
    private K key;                    // The actual index key
    private long valueNodeKey;        // Reference to RBNodeValue
    private long left;                // Left child node key (pointer!)
    private long right;               // Right child node key (pointer!)
    private boolean isChanged;
    private NodeDelegate nodeDelegate; // Contains parent key, etc.
}
```

### Cache Miss Pattern

For a search operation `get(key)`:
1. Load page containing root node → **Cache miss #1**
2. Compare key, decide left/right
3. Load page containing child node → **Cache miss #2** (likely different page)
4. Repeat until found... **O(log n) cache misses**

With ~1M entries, tree height ≈ 20, meaning **~20 random page accesses per lookup**.

---

## Proposed Solutions

### Option A: In-Memory ART Reconstruction

**Concept**: Load persisted RBTree nodes into an in-memory Adaptive Radix Trie (ART) for cache-friendly querying.

#### Why ART?

1. **Already implemented** in `io.sirix.index.art.AdaptiveRadixTree`
2. **O(k) lookup** where k = key length (not tree height)
3. **Cache-friendly**: 
   - Path compression reduces nodes to traverse
   - Dense node types (Node4, Node16, Node48, Node256) fit in cache lines
4. **NavigableMap interface**: Supports range queries, floor/ceiling

#### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         HYBRID STORAGE MODEL                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PERSISTENCE LAYER (unchanged)          QUERY LAYER (new)                  │
│  ┌──────────────────────────┐          ┌─────────────────────────────┐     │
│  │  KeyValueLeafPages       │  ─────>  │  AdaptiveRadixTree<K,V>    │     │
│  │  ├── RBNodeKey records   │ hydrate  │  (in-memory, per-revision)  │     │
│  │  └── RBNodeValue records │          │                             │     │
│  └──────────────────────────┘          └─────────────────────────────┘     │
│                                                                             │
│  On commit:                            On query:                           │
│  - Serialize modified entries          - Use ART (no page access!)         │
│    back to RBTree format              - O(k) lookup, excellent cache      │
│  - Leverage existing versioning                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Implementation Steps

**Phase 1: ARTIndex Wrapper**

```java
package io.sirix.index.art;

/**
 * Index wrapper that uses ART for in-memory operations while
 * persisting changes via the existing RBTree storage format.
 */
public class ARTIndexReader<K extends Comparable<? super K>, V extends References> {
    
    private final AdaptiveRadixTree<K, V> art;
    private final RBTreeReader<K, V> persistedTree;
    private final BinaryComparable<K> binaryComparable;
    private volatile boolean hydrated = false;
    
    /**
     * Lazily hydrate ART from persisted RBTree on first access.
     */
    private void ensureHydrated() {
        if (!hydrated) {
            synchronized (this) {
                if (!hydrated) {
                    hydrateFromRBTree();
                    hydrated = true;
                }
            }
        }
    }
    
    private void hydrateFromRBTree() {
        // Iterate all RBTree nodes, insert into ART
        for (var it = persistedTree.new RBNodeIterator(0); it.hasNext(); ) {
            RBNodeKey<K> node = it.next();
            K key = node.getKey();
            V value = getValueForNode(node);
            art.put(key, value);
        }
    }
    
    public Optional<V> get(K key, SearchMode mode) {
        ensureHydrated();
        return switch (mode) {
            case EQUAL -> Optional.ofNullable(art.get(key));
            case GREATER -> Optional.ofNullable(art.higherEntry(key)).map(Entry::getValue);
            case GREATER_OR_EQUAL -> Optional.ofNullable(art.ceilingEntry(key)).map(Entry::getValue);
            case LESS -> Optional.ofNullable(art.lowerEntry(key)).map(Entry::getValue);
            case LESS_OR_EQUAL -> Optional.ofNullable(art.floorEntry(key)).map(Entry::getValue);
        };
    }
}
```

**Phase 2: BinaryComparable Implementations**

Create `BinaryComparable` adapters for existing key types:

```java
// For PATH index (Long keys)
public class LongBinaryComparable implements BinaryComparable<Long> {
    @Override
    public byte[] get(Long key) {
        // Big-endian encoding preserves sort order
        return new byte[] {
            (byte) (key >> 56), (byte) (key >> 48),
            (byte) (key >> 40), (byte) (key >> 32),
            (byte) (key >> 24), (byte) (key >> 16),
            (byte) (key >> 8),  (byte) key.longValue()
        };
    }
}

// For CAS index (CASValue keys)  
public class CASValueBinaryComparable implements BinaryComparable<CASValue> {
    @Override
    public byte[] get(CASValue key) {
        // Type byte + atomic value encoding + path node key
        ByteBuffer buf = ByteBuffer.allocate(estimateSize(key));
        buf.put((byte) key.getType().ordinal());
        encodeAtomicValue(buf, key.getAtomicValue());
        buf.putLong(key.getPathNodeKey());
        return buf.array();
    }
}

// For NAME index (QNm keys)
public class QNmBinaryComparable implements BinaryComparable<QNm> {
    @Override
    public byte[] get(QNm key) {
        // Namespace URI + local name (both length-prefixed)
        byte[] ns = key.getNamespaceURI().getBytes(UTF_8);
        byte[] local = key.getLocalName().getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + ns.length + 4 + local.length);
        buf.putInt(ns.length).put(ns);
        buf.putInt(local.length).put(local);
        return buf.array();
    }
}
```

**Phase 3: Integration with Existing Index Infrastructure**

```java
// Modify PathIndexReader to use ART internally
public class PathIndexReader implements IndexReader<Long, NodeReferences> {
    
    private final ARTIndexReader<Long, NodeReferences> artReader;
    
    @Override
    public Optional<NodeReferences> find(Long pathNodeKey, SearchMode mode) {
        return artReader.get(pathNodeKey, mode);
    }
}
```

#### Pros & Cons

| Pros | Cons |
|------|------|
| ✅ Uses existing ART implementation | ❌ Memory overhead for hydrated ART |
| ✅ Minimal changes to persistence layer | ❌ Hydration cost on first access |
| ✅ O(k) lookups, cache-friendly | ❌ Dual representation (ART + RBTree) |
| ✅ Easy to implement incrementally | ❌ Must re-hydrate after each commit |
| ✅ NavigableMap operations supported | ❌ Larger working set |

#### Memory Overhead Analysis

For 1M index entries with average key size 20 bytes:
- **ART nodes**: ~40-60 bytes per entry (path compressed) = 40-60 MB
- **Leaf entries**: 20 + 8 bytes (key + value ref) = 28 MB
- **Total**: ~70-90 MB for in-memory ART

For read-heavy workloads, this is acceptable. For write-heavy workloads, consider lazy hydration or partial loading.

---

### Option B: Persistent B+Tree with COW Semantics

**Concept**: Replace the RBTree-in-pages model with a true B+tree where:
- Interior nodes contain only keys and child page references
- Leaf nodes contain key-value pairs
- Copy-on-write at the node level for versioning

#### B+Tree Node Structure

```java
/**
 * B+tree interior node - stores keys and child page references.
 * Fits in a single KeyValueLeafPage slot or page.
 */
public class BPlusInteriorNode<K extends Comparable<? super K>> {
    private static final int MAX_KEYS = 127; // Fits in cache line with references
    
    private K[] keys;                         // Sorted keys
    private long[] childPageKeys;             // Page keys for children
    private int size;                         // Current number of keys
    private long parentPageKey;               // For upward traversal
}

/**
 * B+tree leaf node - stores key-value pairs with sibling links.
 */
public class BPlusLeafNode<K extends Comparable<? super K>, V extends References> {
    private static final int MAX_ENTRIES = 64; // Tuned for cache efficiency
    
    private K[] keys;                          // Sorted keys
    private V[] values;                        // Corresponding values
    private int size;
    private long nextLeafPageKey;              // For range scans
    private long prevLeafPageKey;              // For reverse scans
    private long parentPageKey;
}
```

#### Page Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    B+TREE PAGE ORGANIZATION                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   PathPage                                                                  │
│      └── Root B+Tree Node (Interior)                                       │
│             ├── keys: [100, 200, 300]                                       │
│             └── children: [pageKey1, pageKey2, pageKey3, pageKey4]         │
│                    │          │          │          │                       │
│                    ▼          ▼          ▼          ▼                       │
│              ┌─────────┐┌─────────┐┌─────────┐┌─────────┐                  │
│              │ Leaf 1  ││ Leaf 2  ││ Leaf 3  ││ Leaf 4  │                  │
│              │keys<100 ││100≤k<200││200≤k<300││ k≥300   │                  │
│              │         ││         ││         ││         │                  │
│              │ ◄──────►││◄───────►││◄───────►││◄──────► │ (sibling links) │
│              └─────────┘└─────────┘└─────────┘└─────────┘                  │
│                                                                             │
│   Each leaf contains up to 64 key-value pairs (cache-friendly!)            │
│   Interior nodes enable O(log_64 n) ≈ 3-4 page accesses for 1M entries    │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### COW Versioning Integration

```java
/**
 * B+tree with copy-on-write semantics for Sirix versioning.
 */
public class COWBPlusTree<K extends Comparable<? super K>, V extends References> {
    
    private final StorageEngineWriter pageWriter;
    private final IndexType indexType;
    private final int indexNumber;
    
    public V insert(K key, V value) {
        // 1. Find leaf node containing key
        BPlusLeafNode<K, V> leaf = findLeaf(key);
        
        // 2. Copy-on-write: create new leaf page
        BPlusLeafNode<K, V> newLeaf = copyOnWrite(leaf);
        newLeaf.insert(key, value);
        
        // 3. If leaf overflows, split and propagate up
        if (newLeaf.isOverflow()) {
            splitAndPropagate(newLeaf);
        }
        
        // 4. Update sibling links (also COW)
        updateSiblingLinks(newLeaf);
        
        return value;
    }
    
    private BPlusLeafNode<K, V> copyOnWrite(BPlusLeafNode<K, V> original) {
        // Create new page container in transaction intent log
        long newPageKey = pageWriter.allocateNewPage(indexType, indexNumber);
        BPlusLeafNode<K, V> copy = original.deepCopy();
        copy.setPageKey(newPageKey);
        
        // Store in TIL for commit
        pageWriter.preparePage(newPageKey, copy);
        
        return copy;
    }
}
```

#### Versioning Algorithm Adaptation

The existing versioning algorithms (DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT) can be adapted:

```java
// In VersioningType.java, add B+tree-aware methods

public enum VersioningType {
    DIFFERENTIAL {
        @Override
        public <K, V> BPlusNode<K, V> combineBPlusNodes(
            List<BPlusNode<K, V>> versions,
            int revToRestore,
            StorageEngineReader pageReadTrx
        ) {
            // Latest version + full dump reference
            BPlusNode<K, V> latest = versions.get(0);
            BPlusNode<K, V> fullDump = versions.size() > 1 ? versions.get(1) : latest;
            
            // Merge entries: latest wins, fill gaps from fullDump
            return mergeNodes(latest, fullDump);
        }
    }
    // ... similar for other versioning types
}
```

#### Implementation Steps

**Phase 1: Node Serialization**

```java
// BPlusNodeSerializer.java
public class BPlusNodeSerializer {
    
    public void serializeInterior(BytesOut out, BPlusInteriorNode<?> node) {
        out.writeByte(NodeKind.BPLUS_INTERIOR.getId());
        out.writeInt(node.getSize());
        for (int i = 0; i < node.getSize(); i++) {
            serializeKey(out, node.getKey(i));
        }
        for (int i = 0; i <= node.getSize(); i++) {
            out.writeLong(node.getChildPageKey(i));
        }
    }
    
    public void serializeLeaf(BytesOut out, BPlusLeafNode<?, ?> node) {
        out.writeByte(NodeKind.BPLUS_LEAF.getId());
        out.writeInt(node.getSize());
        out.writeLong(node.getNextLeafPageKey());
        out.writeLong(node.getPrevLeafPageKey());
        for (int i = 0; i < node.getSize(); i++) {
            serializeKey(out, node.getKey(i));
            serializeValue(out, node.getValue(i));
        }
    }
}
```

**Phase 2: B+Tree Reader/Writer**

```java
public class BPlusTreeWriter<K extends Comparable<? super K>, V extends References> {
    
    private static final int INTERIOR_ORDER = 128;  // Max children per interior node
    private static final int LEAF_ORDER = 64;       // Max entries per leaf
    
    private final StorageEngineWriter pageWriter;
    private long rootPageKey;
    
    public void insert(K key, V value) {
        if (rootPageKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
            // Create first leaf as root
            createInitialLeaf(key, value);
            return;
        }
        
        // Descend to leaf, COW along the path
        Deque<BPlusNode<K, V>> path = new ArrayDeque<>();
        BPlusLeafNode<K, V> leaf = findLeafWithPath(key, path);
        
        // Insert into leaf (COW)
        BPlusLeafNode<K, V> newLeaf = insertIntoLeaf(leaf, key, value);
        
        // Handle splits
        if (newLeaf.getSize() > LEAF_ORDER) {
            splitLeafAndPropagate(newLeaf, path);
        }
    }
    
    private void splitLeafAndPropagate(BPlusLeafNode<K, V> leaf, Deque<BPlusNode<K, V>> path) {
        // Split leaf into two
        K midKey = leaf.getKey(LEAF_ORDER / 2);
        BPlusLeafNode<K, V> rightLeaf = leaf.splitRight(LEAF_ORDER / 2);
        
        // Allocate new page for right leaf
        long rightPageKey = allocatePage();
        rightLeaf.setPageKey(rightPageKey);
        
        // Update sibling links
        rightLeaf.setPrevLeafPageKey(leaf.getPageKey());
        rightLeaf.setNextLeafPageKey(leaf.getNextLeafPageKey());
        leaf.setNextLeafPageKey(rightPageKey);
        
        // Propagate split up to parent
        propagateSplit(path, midKey, rightPageKey);
    }
}
```

**Phase 3: Integration**

Replace RBTree usage in index readers/writers:

```java
// Before: RBTreeReader<Long, NodeReferences>
// After:  BPlusTreeReader<Long, NodeReferences>

public class PathIndexReader {
    private final BPlusTreeReader<Long, NodeReferences> bplusTree;
    
    public Optional<NodeReferences> find(Long pathNodeKey) {
        return bplusTree.get(pathNodeKey);
    }
    
    public Iterator<Entry<Long, NodeReferences>> range(Long from, Long to) {
        return bplusTree.rangeIterator(from, to);  // Very efficient with leaf links!
    }
}
```

#### Pros & Cons

| Pros | Cons |
|------|------|
| ✅ Excellent cache locality (64 entries/leaf) | ❌ Major refactoring required |
| ✅ O(log₆₄ n) ≈ 3-4 page accesses | ❌ More complex COW logic |
| ✅ Efficient range scans via leaf links | ❌ Split/merge propagation complexity |
| ✅ Industry-proven design | ❌ New serialization format |
| ✅ Single representation (no hydration) | ❌ Migration path needed |

#### Performance Comparison

| Metric | Current RBTree | B+Tree (order 64) |
|--------|---------------|-------------------|
| Tree height for 1M entries | ~20 | ~4 |
| Page accesses per lookup | ~20 | ~4 |
| Entries per page | 1-2 | 64 |
| Cache efficiency | Poor | Excellent |
| Range scan efficiency | Poor (tree walk) | Excellent (leaf links) |

---

### Option C: Hybrid Approach - Sorted Arrays in Leaf Pages

**Concept**: Keep the existing trie structure but replace RBTree nodes with sorted arrays within each `KeyValueLeafPage`. This is a middle ground requiring less refactoring.

#### Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                  SORTED ARRAY LEAF PAGE DESIGN                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   KeyValueLeafPage (INDEX type)                                            │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ Header: count, keyType, metadata                                    │  │
│   ├─────────────────────────────────────────────────────────────────────┤  │
│   │ Sorted Key Array:    [key₀] [key₁] [key₂] ... [key_{n-1}]          │  │
│   │                         │      │      │            │                │  │
│   │ Value Array:         [val₀] [val₁] [val₂] ... [val_{n-1}]          │  │
│   ├─────────────────────────────────────────────────────────────────────┤  │
│   │ Sibling Links: prevPageKey, nextPageKey                             │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   Binary search within page: O(log 1024) ≈ 10 comparisons                 │
│   Sequential scan for ranges: excellent cache locality                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Implementation

```java
/**
 * Sorted array index stored in a KeyValueLeafPage.
 * Replaces RBTree nodes with contiguous sorted storage.
 */
public class SortedArrayIndex<K extends Comparable<? super K>, V extends References> {
    
    private static final int MAX_ENTRIES = 256;  // Per page
    
    private final K[] keys;
    private final V[] values;
    private int size;
    private long prevPageKey;
    private long nextPageKey;
    
    /**
     * Binary search for key position.
     * @return index if found, or -(insertionPoint + 1) if not found
     */
    public int binarySearch(K key) {
        int low = 0, high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = keys[mid].compareTo(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }
    
    /**
     * Insert key-value pair, maintaining sort order.
     * Returns true if page split is needed.
     */
    public boolean insert(K key, V value) {
        int pos = binarySearch(key);
        if (pos >= 0) {
            // Key exists, update value
            values[pos] = value;
            return false;
        }
        
        int insertPos = -(pos + 1);
        
        // Shift elements right
        System.arraycopy(keys, insertPos, keys, insertPos + 1, size - insertPos);
        System.arraycopy(values, insertPos, values, insertPos + 1, size - insertPos);
        
        keys[insertPos] = key;
        values[insertPos] = value;
        size++;
        
        return size > MAX_ENTRIES;
    }
    
    /**
     * Efficient range iterator using cache-friendly sequential access.
     */
    public Iterator<Entry<K, V>> rangeIterator(K from, K to) {
        int startPos = from == null ? 0 : Math.max(0, -(binarySearch(from) + 1));
        return new Iterator<>() {
            int pos = startPos;
            
            @Override
            public boolean hasNext() {
                return pos < size && (to == null || keys[pos].compareTo(to) <= 0);
            }
            
            @Override
            public Entry<K, V> next() {
                return new AbstractMap.SimpleEntry<>(keys[pos], values[pos++]);
            }
        };
    }
}
```

#### Migration Strategy

```java
/**
 * Migrate RBTree index to sorted array format.
 */
public class IndexMigrator {
    
    public void migrateToSortedArray(
        RBTreeReader<?, ?> oldTree,
        SortedArrayIndexWriter writer
    ) {
        // In-order traversal produces sorted output
        List<Entry<?, ?>> sorted = new ArrayList<>();
        for (var it = oldTree.new RBNodeIterator(0); it.hasNext(); ) {
            RBNodeKey<?> node = it.next();
            sorted.add(new SimpleEntry<>(node.getKey(), getValueForNode(node)));
        }
        
        // Bulk load into sorted array pages
        writer.bulkLoad(sorted);
    }
}
```

#### Pros & Cons

| Pros | Cons |
|------|------|
| ✅ Reuses KeyValueLeafPage infrastructure | ❌ O(n) insert/delete (shifting) |
| ✅ Excellent cache locality within page | ❌ Still requires trie traversal to find page |
| ✅ Simple implementation | ❌ May need page splits on insert |
| ✅ Easy migration from RBTree | ❌ Less efficient than B+tree for large datasets |
| ✅ Good for small-medium indexes | |

---

## Recommendation

### Short-term (Low Risk): Option A - ART Reconstruction

**Why?**
1. Already have working ART implementation
2. No changes to persistence format
3. Incremental adoption possible
4. Immediate cache-friendliness benefits

**Implementation effort**: 2-3 weeks

### Long-term (High Impact): Option B - B+Tree

**Why?**
1. Industry-standard for database indexes
2. Best balance of read/write performance
3. Excellent range query support
4. Single representation (no hydration overhead)

**Implementation effort**: 6-8 weeks

---

## Implementation Roadmap

### Phase 1: ART-based Index Reader (Weeks 1-3)

1. Create `BinaryComparable` implementations for `Long`, `CASValue`, `QNm`
2. Implement `ARTIndexReader<K, V>` wrapper
3. Add lazy hydration from persisted RBTree
4. Integrate with `PathIndexReader`, `CASIndexReader`, `NameIndexReader`
5. Benchmark against current implementation

### Phase 2: ART-based Index Writer (Weeks 4-5)

1. Implement `ARTIndexWriter<K, V>` with change tracking
2. Serialize modified entries back to RBTree format on commit
3. Handle concurrent readers during write

### Phase 3: B+Tree Design & Prototype (Weeks 6-8)

1. Design `BPlusInteriorNode` and `BPlusLeafNode` classes
2. Implement node serialization/deserialization
3. Create `BPlusTreeReader` with page-based access
4. Test COW semantics integration

### Phase 4: B+Tree Writer & Migration (Weeks 9-12)

1. Implement `BPlusTreeWriter` with split/merge logic
2. Adapt versioning algorithms for B+tree nodes
3. Create migration tool from RBTree to B+tree format
4. Performance testing and optimization

### Phase 5: Production Rollout (Weeks 13-14)

1. Feature flag for B+tree vs RBTree
2. Documentation updates
3. Migration guide for existing databases

---

## Metrics for Success

| Metric | Current | Target (ART) | Target (B+tree) |
|--------|---------|--------------|-----------------|
| Page accesses per lookup | ~20 | ~3-5 (after hydration) | ~4 |
| Range query efficiency | O(k × log n) | O(k + m) | O(log n + m) |
| Memory overhead | Low | +50-80 MB for 1M entries | Similar to current |
| Insert latency | O(log n) page accesses | O(log n) + O(k) | O(log n / 64) |
| Cache hit rate | ~30-40% | ~80-90% | ~85-95% |

---

## Appendix: ART Node Types (Reference)

The existing `AdaptiveRadixTree` implementation uses these node types:

| Type | Capacity | Structure | Cache Behavior |
|------|----------|-----------|----------------|
| `Node4` | 4 children | 4 keys + 4 pointers | Fits in 1 cache line |
| `Node16` | 16 children | 16 keys + 16 pointers | SIMD-friendly search |
| `Node48` | 48 children | 256-byte key map + 48 pointers | Constant-time lookup |
| `Node256` | 256 children | 256 pointers | Direct indexing |
| `LeafNode` | Key-Value | Full key + value | Terminal node |

All node types support path compression for common prefixes, further reducing traversal depth.

---

## References

1. "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases" - Viktor Leis et al., 2013
2. "Modern B-Tree Techniques" - Goetz Graefe, Foundations and Trends in Databases, 2011
3. Sirix existing ART implementation: `io.sirix.index.art.AdaptiveRadixTree`
4. Sirix RBTree implementation: `io.sirix.index.redblacktree.RBTreeReader/Writer`


