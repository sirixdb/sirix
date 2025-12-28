# Cache-Friendly Secondary Index Structure Plan

## Executive Summary

The current secondary index implementation uses Red-Black Trees (RBTree) stored as individual node records within `KeyValueLeafPage`s in a trie structure. This design has significant cache-friendliness issues:

1. **Pointer-chasing**: Each tree node stores only key references (`leftChildKey`, `rightChildKey`), requiring separate page lookups per traversal step
2. **Poor locality**: Related keys can be scattered across different `KeyValueLeafPage`s
3. **Random I/O**: Tree traversal pattern leads to unpredictable page access
4. **On-heap allocations**: Per-entry `byte[]` creates GC pressure during deserialization

This document proposes **four alternative approaches** with detailed implementation plans:

- **Option A**: In-memory ART reconstruction (short-term, low risk)
- **Option B**: Persistent B+Tree with COW semantics
- **Option C**: Sorted arrays in leaf pages (hybrid approach)
- **Option D**: Persistent HOT (Height Optimized Trie) - **recommended long-term**

Additionally, **Appendix B** describes off-heap leaf storage via `MemorySegment` that can be combined with any option to eliminate GC pressure and enable zero-copy deserialization.

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

### Option D: Persistent HOT (Height Optimized Trie)

**Concept**: Replace the RBTree-in-pages model with a persistent HOT (Height Optimized Trie), which combines multiple trie levels into compound nodes for superior cache efficiency and reduced tree height.

**Key Design Principle**: HOT integrates with SirixDB's existing page infrastructure:
- **HOTIndirectPage** → analogous to `IndirectPage` (interior nodes with child page references)
- **HOTLeafPage** → analogous to `KeyValueLeafPage` (versioned leaf nodes with key-value entries)
- Both page types go through the **same versioning pipeline** (FULL, DIFFERENTIAL, INCREMENTAL, SLIDING_SNAPSHOT)

#### Why HOT over B+Tree?

HOT was introduced in "HOT: A Height Optimized Trie Index for Main-Memory Database Systems" (Binna et al., SIGMOD 2018). It offers several advantages over B+Trees for main-memory/off-heap scenarios:

| Aspect | B+Tree | HOT (COW-adapted) |
|--------|--------|-------------------|
| Lookup complexity | O(log n) comparisons | O(k) where k = key length |
| Tree height (1M entries) | ~4 levels | ~2-3 levels |
| SIMD utilization | Limited | Designed for SIMD |
| Key comparisons | Full key each level | Partial key (discriminative bits) |
| Space efficiency | Fixed fanout waste | Adaptive node sizes |
| Ordered iteration | ✅ via leaf links | ✅ via in-order trie traversal (COW-safe) |
| COW compatibility | ❌ sibling links cascade | ✅ no sibling links needed |

#### Architecture: Alignment with SirixDB Page Model

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              HOT TRIE vs CURRENT TRIE ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   CURRENT MODEL                        HOT MODEL (COW-compatible)           │
│   ─────────────────                    ───────────────────────────          │
│                                                                             │
│   PathPage/CASPage/NamePage            PathPage/CASPage/NamePage            │
│      │                                    │                                 │
│      ▼                                    ▼                                 │
│   IndirectPage (trie level 0)          HOTIndirectPage (compound node)      │
│      │                                    │                                 │
│      ▼                                    │  ← Fewer levels due to          │
│   IndirectPage (trie level 1)             │    compound nodes!              │
│      │                                    │                                 │
│      ▼                                    ▼                                 │
│   IndirectPage (trie level 2)          HOTIndirectPage (compound node)      │
│      │                                    │                                 │
│      ▼                                    ├────────┬────────┐               │
│   ...more levels...                       ▼        ▼        ▼               │
│      │                                 HOTLeafPage  HOTLeafPage  HOTLeafPage│
│      ▼                                 (versioned, no sibling pointers!)    │
│   KeyValueLeafPage                                                          │
│      │                                 Range scans: parent-based in-order   │
│      ▼                                 traversal using HOTRangeCursor       │
│   RBTree nodes (scattered)                                                  │
│                                                                             │
│   VERSIONING: Both models use same VersioningType.combineRecordPages()     │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### HOT Page Types

**1. HOTIndirectPage** (analogous to `IndirectPage`)

Stores HOT interior nodes (BiNode, SpanNode, MultiNode) with child page references:

```java
/**
 * HOT interior page - stores compound nodes spanning multiple trie levels.
 * Analogous to IndirectPage but with HOT-specific node types.
 * 
 * NOT versioned at fragment level - only referenced pages are versioned.
 */
public final class HOTIndirectPage extends AbstractForwardingPage {
    
    public enum NodeType {
        BI_NODE,      // 2 children, 1 discriminative bit
        SPAN_NODE,    // 2-16 children, SIMD-optimized
        MULTI_NODE    // 17-256 children, direct indexing
    }
    
    private final NodeType nodeType;
    private final PageReference[] childReferences;  // References to child pages
    
    // BiNode fields
    private int discriminativeBitPos;
    
    // SpanNode fields  
    private byte[] partialKeys;        // Up to 16 partial keys for SIMD search
    private short[] bitPositions;      // Which bits are discriminative
    
    // MultiNode fields
    private byte[] childIndex;         // 256-byte direct index
    
    /**
     * Find child page reference for given key.
     */
    public PageReference findChild(byte[] key) {
        return switch (nodeType) {
            case BI_NODE -> findChildBiNode(key);
            case SPAN_NODE -> findChildSpanNode(key);  // SIMD-optimized
            case MULTI_NODE -> findChildMultiNode(key);
        };
    }
}
```

**2. HOTLeafPage** (analogous to `KeyValueLeafPage` - **VERSIONED**)

Stores key-value entries with full versioning support:

```java
/**
 * HOT leaf page - stores sorted key-value entries.
 * Analogous to KeyValueLeafPage with FULL versioning support.
 * 
 * VERSIONED: Goes through combineRecordPagesForModification() like KeyValueLeafPage.
 * Uses off-heap MemorySegment storage (Appendix B).
 * 
 * NOTE: NO sibling pointers! COW architecture prohibits sibling links because
 * modifying one leaf would cascade COW to all siblings. Range scans use
 * parent-based in-order traversal instead (see HOTRangeCursor).
 */
public final class HOTLeafPage implements Page {
    
    // ===== Same versioning infrastructure as KeyValueLeafPage =====
    private final long recordPageKey;
    private final int revision;
    private final IndexType indexType;
    private final PageDelegate delegate;
    
    // ===== Off-heap storage (see Appendix B) =====
    private final MemorySegment slotMemory;      // Key-value entries
    private final Runnable releaser;
    private final int[] slotOffsets;             // Sorted by key
    private int entryCount;
    
    // ===== NO sibling links - COW incompatible! =====
    // Range scans use HOTRangeCursor with parent stack instead
    
    // ===== Guard-based lifetime management =====
    private final AtomicInteger guardCount = new AtomicInteger(0);
    private volatile boolean closed = false;
    
    /**
     * Binary search for key (O(log n) within page).
     */
    public int findEntry(byte[] key) {
        int low = 0, high = entryCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            byte[] midKey = getKey(mid);
            int cmp = compareKeys(midKey, key);
            if (cmp < 0) low = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else return mid;
        }
        return -(low + 1);
    }
    
    /**
     * Zero-copy key access from off-heap segment.
     */
    public MemorySegment getKeySlice(int index) {
        int offset = slotOffsets[index];
        int keyLen = slotMemory.get(ValueLayout.JAVA_SHORT, offset);
        return slotMemory.asSlice(offset + 2, keyLen);
    }
    
    /**
     * Zero-copy value access from off-heap segment.
     */
    public MemorySegment getValueSlice(int index) {
        int offset = slotOffsets[index];
        int keyLen = slotMemory.get(ValueLayout.JAVA_SHORT, offset);
        int valueOffset = slotMemory.get(ValueLayout.JAVA_INT, offset + 2 + keyLen);
        int valueLen = slotMemory.get(ValueLayout.JAVA_SHORT, valueOffset);
        return slotMemory.asSlice(valueOffset + 2, valueLen);
    }
    
    // ===== Versioning integration =====
    
    @Override
    public PageReference[] getReferences() {
        return delegate.getReferences();
    }
    
    @Override 
    public void setReference(int index, PageReference ref) {
        delegate.setReference(index, ref);
    }
}
```

#### Versioning Integration

HOTLeafPage uses the **same versioning pipeline** as KeyValueLeafPage:

```java
// In VersioningType.java - HOTLeafPage goes through same combine logic

public enum VersioningType {
    
    DIFFERENTIAL {
        @Override
        public PageContainer combineRecordPagesForModification(
            List<KeyValuePage> fragments,  // Can be KeyValueLeafPage OR HOTLeafPage
            int revToRestore,
            StorageEngineReader pageReadTrx,
            PageReference ref,
            TransactionIntentLog log
        ) {
            // Same logic works for both page types!
            // Fragment[0] = latest version, Fragment[1] = full dump
            KeyValuePage completePage = combineFragments(fragments);
            KeyValuePage modifyingPage = copyOnWrite(completePage);
            return new PageContainer(completePage, modifyingPage);
        }
    },
    
    // INCREMENTAL, SLIDING_SNAPSHOT, FULL - all work with HOTLeafPage
}
```

#### PageKind Extension

```java
// Add HOT page types to PageKind enum

public enum PageKind {
    // Existing types
    KEYVALUELEAFPAGE((byte) 2) { ... },
    INDIRECTPAGE((byte) 3) { ... },
    
    // New HOT types
    HOT_INDIRECT_PAGE((byte) 10) {
        @Override
        public Page deserializePage(BytesIn source, SerializationType type,
                                    ResourceConfiguration config) {
            return HOTIndirectPage.deserialize(source, type, config);
        }
        
        @Override
        public void serializePage(BytesOut sink, Page page, 
                                  SerializationType type) {
            HOTIndirectPage.serialize(sink, (HOTIndirectPage) page, type);
        }
    },
    
    HOT_LEAF_PAGE((byte) 11) {
        @Override
        public Page deserializePage(BytesIn source, SerializationType type,
                                    ResourceConfiguration config) {
            // Zero-copy deserialization to off-heap MemorySegment
            return HOTLeafPage.deserializeZeroCopy(source, type, config);
        }
        
        @Override
        public void serializePage(BytesOut sink, Page page,
                                  SerializationType type) {
            HOTLeafPage hotLeaf = (HOTLeafPage) page;
            // Stream raw MemorySegment bytes
            hotLeaf.serializeToSink(sink);
        }
    };
}
```

#### Memory Layout: HOTLeafPage (Off-Heap, Versioned)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HOTLeafPage MEMORY LAYOUT                                │
│              (Off-heap MemorySegment, 64KB default)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   Header (24 bytes) - mirrors KeyValueLeafPage                              │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [u8  pageKind = HOT_LEAF_PAGE]                                      │  │
│   │ [u8  version]                                                       │  │
│   │ [i64 recordPageKey]        - page key for versioning                │  │
│   │ [i32 revision]             - revision number                        │  │
│   │ [u8  indexType]            - PATH/CAS/NAME                          │  │
│   │ [i32 entryCount]           - number of key-value pairs              │  │
│   │ [u8  minKeyLen]            - for parent navigation (optional)       │  │
│   │ [u8  maxKeyLen]            - for parent navigation (optional)       │  │
│   │                                                                     │  │
│   │ NOTE: NO sibling pointers! COW-incompatible.                        │  │
│   │       Range scans use parent-based in-order traversal.              │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   Slot Offsets (4 bytes × entryCount) - sorted by key                      │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [i32 offset[0]] [i32 offset[1]] ... [i32 offset[n-1]]               │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   Slot Memory (key-value entries, packed)                                   │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ Entry format:                                                       │  │
│   │   [u16 keyLen][key bytes][u16 valueLen][value bytes]                │  │
│   │                                                                     │  │
│   │ Value = serialized NodeReferences (nodeKey list)                    │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│   Versioning Bitmap (128 bytes) - same as KeyValueLeafPage                  │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [entriesBitmap: 16 longs] - which slots are present                 │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Range Scans: Parent-Based In-Order Traversal (COW-Compatible)

Since sibling pointers are incompatible with COW, range scans use **in-order trie traversal** with a parent stack:

```java
/**
 * COW-compatible range cursor using parent-based navigation.
 * No sibling pointers needed - traverses HOT structure in sorted order.
 */
public class HOTRangeCursor<K, V> implements Iterator<Entry<K, V>> {
    
    private final HOTIndexReader reader;
    private final byte[] fromKey;
    private final byte[] toKey;
    
    // Parent stack for in-order traversal (no sibling links!)
    private final Deque<TraversalState> parentStack = new ArrayDeque<>();
    
    // Current position
    private HOTLeafPage currentLeaf;
    private int currentIndex;
    private boolean exhausted = false;
    
    private record TraversalState(
        HOTIndirectPage node,
        int childIndex,      // Which child we came from
        int numChildren      // Total children in this node
    ) {}
    
    public HOTRangeCursor(HOTIndexReader reader, K from, K to) {
        this.reader = reader;
        this.fromKey = reader.keyToBytes(from);
        this.toKey = reader.keyToBytes(to);
        
        // Descend to first leaf >= fromKey, recording parent path
        descendToFirstLeaf();
    }
    
    private void descendToFirstLeaf() {
        Page current = reader.getRootPage();
        
        while (current instanceof HOTIndirectPage indirect) {
            int childIdx = indirect.findChildIndex(fromKey);
            int numChildren = indirect.getNumChildren();
            
            // Push parent state for backtracking
            parentStack.push(new TraversalState(indirect, childIdx, numChildren));
            
            // Descend to child
            PageReference childRef = indirect.getChildReference(childIdx);
            current = reader.loadPage(childRef);
        }
        
        // Now at leaf
        currentLeaf = (HOTLeafPage) current;
        currentIndex = currentLeaf.findInsertPos(fromKey);
        if (currentIndex < 0) {
            currentIndex = -(currentIndex + 1);
        }
    }
    
    @Override
    public boolean hasNext() {
        if (exhausted) return false;
        
        // Advance within current leaf if possible
        while (currentIndex >= currentLeaf.getEntryCount()) {
            // Need to move to next leaf via parent traversal
            if (!advanceToNextLeaf()) {
                exhausted = true;
                return false;
            }
        }
        
        // Check if current key is within range
        byte[] currentKey = currentLeaf.getKeyBytes(currentIndex);
        if (compareKeys(currentKey, toKey) > 0) {
            exhausted = true;
            return false;
        }
        
        return true;
    }
    
    /**
     * Move to next leaf using parent stack (no sibling pointers!).
     * This is in-order traversal of the HOT trie.
     */
    private boolean advanceToNextLeaf() {
        // Pop up to find a parent with more children to visit
        while (!parentStack.isEmpty()) {
            TraversalState state = parentStack.pop();
            int nextChildIdx = state.childIndex + 1;
            
            if (nextChildIdx < state.numChildren) {
                // This parent has more children - descend to next one
                parentStack.push(new TraversalState(
                    state.node, nextChildIdx, state.numChildren
                ));
                
                // Descend to leftmost leaf of this subtree
                Page current = reader.loadPage(
                    state.node.getChildReference(nextChildIdx)
                );
                
                while (current instanceof HOTIndirectPage indirect) {
                    parentStack.push(new TraversalState(
                        indirect, 0, indirect.getNumChildren()
                    ));
                    current = reader.loadPage(indirect.getChildReference(0));
                }
                
                currentLeaf = (HOTLeafPage) current;
                currentIndex = 0;
                return true;
            }
            // Else: this parent exhausted, continue popping
        }
        
        // Stack empty - no more leaves
        return false;
    }
    
    @Override
    public Entry<K, V> next() {
        if (!hasNext()) throw new NoSuchElementException();
        
        K key = reader.bytesToKey(currentLeaf.getKeyBytes(currentIndex));
        V value = reader.deserializeValue(currentLeaf.getValueSlice(currentIndex));
        currentIndex++;
        
        return new SimpleEntry<>(key, value);
    }
}
```

**Why this works with COW:**
- No pointers between sibling leaves → modifying one leaf doesn't affect others
- Parent references are **read-only** during iteration (loaded via versioning)
- Each leaf is independently versioned
- Range scan correctness maintained via in-order trie traversal

#### HOT Interior Node Types (in HOTIndirectPage)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    HOT COMPOUND NODE TYPES                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   BiNode (2 children) - 1 discriminative bit                                │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [u16 bitPosition]                                                   │  │
│   │ [PageReference leftChild]   - bit=0                                 │  │
│   │ [PageReference rightChild]  - bit=1                                 │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│   Size: ~24 bytes, fits in 1 cache line                                     │
│                                                                             │
│   SpanNode (2-16 children) - SIMD-optimized                                 │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [u8 numChildren]                                                    │  │
│   │ [u8 numDiscriminativeBits]                                          │  │
│   │ [u16[] bitPositions]        - which bits to extract                 │  │
│   │ [u8[16] partialKeys]        - SIMD-searchable (ByteVector)          │  │
│   │ [PageReference[] children]  - up to 16 child refs                   │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│   Size: ~200 bytes, SIMD search in <10 cycles                               │
│                                                                             │
│   MultiNode (17-256 children) - direct indexing                             │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │ [u8 numChildren]                                                    │  │
│   │ [u8 discriminativeByte]     - which byte of key to use              │  │
│   │ [u8[256] childIndex]        - maps byte value → child slot          │  │
│   │ [PageReference[] children]  - compact array of refs                 │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│   Size: ~256 + children*8 bytes, O(1) lookup                                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### COW Semantics with SirixDB Versioning

HOTLeafPage integrates with the existing `NodeStorageEngineWriter` and `TransactionIntentLog`:

```java
/**
 * HOT index writer - uses SirixDB's versioning infrastructure.
 */
public class HOTIndexWriter<K, V> {
    
    private final NodeStorageEngineWriter storageEngine;
    private final TransactionIntentLog transactionLog;
    private final IndexType indexType;
    private final int indexNumber;
    private long rootPageKey;
    
    public V insert(K key, V value) {
        byte[] keyBytes = keyToBytes(key);
        
        // 1. Get leaf page through versioning pipeline (same as getRecord())
        PageReference leafRef = findLeafReference(keyBytes);
        
        // 2. Get page container through VersioningType.combineRecordPagesForModification()
        //    This handles FULL/DIFFERENTIAL/INCREMENTAL/SLIDING_SNAPSHOT
        PageContainer container = storageEngine.prepareLeafPageForModification(
            leafRef, indexType, indexNumber
        );
        
        HOTLeafPage completePage = (HOTLeafPage) container.getCompletePage();
        HOTLeafPage modifyingPage = (HOTLeafPage) container.getModifiedPage();
        
        // 3. Insert into the modifying page (off-heap MemorySegment)
        boolean needsSplit = modifyingPage.insert(keyBytes, serializeValue(value));
        
        // 4. Handle splits if needed
        if (needsSplit) {
            handleLeafSplit(modifyingPage, leafRef);
        }
        
        // 5. Page automatically tracked in TransactionIntentLog
        //    Will be serialized on commit via PageKind.HOT_LEAF_PAGE
        
        return value;
    }
    
    /**
     * Leaf split creates new versioned pages.
     * NOTE: No sibling pointers - COW incompatible!
     */
    private void handleLeafSplit(HOTLeafPage fullPage, PageReference originalRef) {
        // Allocate new page key
        long newPageKey = storageEngine.getNewPageKey();
        
        // Create new HOTLeafPage with off-heap segment
        var allocation = storageEngine.getAllocator().allocate(HOTLeafPage.DEFAULT_SIZE);
        HOTLeafPage rightPage = new HOTLeafPage(
            newPageKey,
            storageEngine.getRevision(),
            indexType,
            allocation.segment(),
            allocation.releaser()
        );
        
        // Split entries (right half goes to new page)
        byte[] splitKey = fullPage.splitTo(rightPage);
        
        // NO sibling link updates! COW-incompatible.
        // Range scans use parent-based in-order traversal instead.
        
        // Add to transaction log (versioned!)
        transactionLog.put(new PageReference(newPageKey), 
            new PageContainer(rightPage, rightPage));
        
        // Update parent HOTIndirectPage (COW propagation)
        // Parent now has two children instead of one
        propagateSplitToParent(fullPage.getPageKey(), newPageKey, splitKey);
    }
    
    /**
     * Range query using parent-based in-order traversal (COW-compatible).
     * No sibling pointers - uses HOTRangeCursor instead.
     */
    public Iterator<Entry<K, V>> range(K fromKey, K toKey) {
        return new HOTRangeCursor<>(this, fromKey, toKey);
    }
}
```

#### Fragment Combining for HOTLeafPage

HOTLeafPage works with existing versioning algorithms:

```java
// In VersioningType.java

DIFFERENTIAL {
    @Override
    public PageContainer combineRecordPagesForModification(
        List<KeyValuePage> fragments,
        int revToRestore,
        StorageEngineReader reader,
        PageReference ref,
        TransactionIntentLog log
    ) {
        // Works for both KeyValueLeafPage AND HOTLeafPage!
        KeyValuePage latestFragment = fragments.get(0);
        KeyValuePage fullDump = fragments.size() > 1 ? fragments.get(1) : latestFragment;
        
        // Combine using slot bitmap (same logic for both page types)
        KeyValuePage completePage = combineWithBitmap(latestFragment, fullDump);
        
        // COW: allocate new page for modifications
        KeyValuePage modifyingPage = copyOnWrite(completePage, reader);
        
        return new PageContainer(completePage, modifyingPage);
    }
}

// HOTLeafPage implements KeyValuePage interface for compatibility
public final class HOTLeafPage implements KeyValuePage {
    
    @Override
    public void setSlot(int slot, byte[] data) {
        // Write to off-heap MemorySegment
        int offset = allocateSlot(data.length);
        MemorySegment.copy(data, 0, slotMemory, offset, data.length);
        slotOffsets[slot] = offset;
    }
    
    @Override
    public MemorySegment getSlotAsSegment(int slot) {
        int offset = slotOffsets[slot];
        int length = getSlotLength(slot);
        return slotMemory.asSlice(offset, length);
    }
    
    @Override
    public long[] getSlotBitmap() {
        return slotBitmap;  // Same 128-byte bitmap as KeyValueLeafPage
    }
}
```

#### HOT Node Implementation (SIMD-Friendly)

```java
/**
 * SpanNode: handles 2-16 children with SIMD-optimized search.
 */
public class HOTSpanNode {
    
    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_128;
    
    private final MemorySegment segment;
    private final int numChildren;
    private final int numDiscriminativeBits;
    
    /**
     * SIMD-accelerated child lookup using discriminative bits.
     */
    public int findChild(byte[] key) {
        // Extract discriminative bits from key
        byte partialKey = extractPartialKey(key);
        
        // Load partial keys from segment (up to 16 bytes)
        ByteVector partialKeys = ByteVector.fromMemorySegment(
            SPECIES, segment, PARTIAL_KEYS_OFFSET, ByteOrder.LITTLE_ENDIAN
        );
        
        // Broadcast search key
        ByteVector searchKey = ByteVector.broadcast(SPECIES, partialKey);
        
        // SIMD compare: find matching position
        VectorMask<Byte> matches = partialKeys.eq(searchKey);
        int matchIndex = matches.firstTrue();
        
        return matchIndex < numChildren ? matchIndex : -1;
    }
    
    private byte extractPartialKey(byte[] key) {
        // Extract only the discriminative bits into a compact byte
        byte result = 0;
        for (int i = 0; i < numDiscriminativeBits; i++) {
            int bitPos = getDiscriminativeBitPos(i);
            int byteIndex = bitPos / 8;
            int bitIndex = bitPos % 8;
            if ((key[byteIndex] & (1 << bitIndex)) != 0) {
                result |= (1 << i);
            }
        }
        return result;
    }
}
```

#### Pros & Cons

| Pros | Cons |
|------|------|
| ✅ Superior cache efficiency (compound nodes) | ❌ More complex than B+Tree |
| ✅ O(k) lookup, not O(log n) | ❌ Novel data structure (less battle-tested) |
| ✅ SIMD-optimized node search | ❌ Requires SIMD intrinsics for full benefit |
| ✅ Lower tree height than B+Tree | ❌ Split/merge logic more involved |
| ✅ Space-efficient (adaptive node sizes) | ❌ More complex serialization |
| ✅ Excellent for off-heap storage | ❌ Implementation effort ~8-10 weeks |
| ✅ **COW-compatible** (no sibling pointers) | ❌ Range scans slightly slower than sibling links |
| ✅ Range queries via in-order traversal | |

#### Performance Comparison

| Metric | Current RBTree | B+Tree | HOT (COW-adapted) |
|--------|---------------|--------|-------------------|
| Tree height (1M entries) | ~20 | ~4 | **~2-3** |
| Page accesses per lookup | ~20 | ~4 | **~2-3** |
| Key comparisons per lookup | O(log n) | O(log n) | **O(k)** |
| SIMD utilization | None | Limited | **Designed for SIMD** |
| Cache lines per lookup | ~20 | ~4-8 | **~2-4** |
| Range scan method | Tree walk | Sibling links | **In-order traversal** |
| Range scan (m results) | O(m × log n) | O(log n + m) | O(log n + m × h/m)* |
| COW compatibility | ✅ | ❌ siblings cascade | **✅ no siblings** |

*Range scan amortizes to ~O(log n + m) since parent stack is shallow (h ≈ 2-3)

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

### Long-term (High Impact): Option D - Persistent HOT

**Why?**
1. Superior cache efficiency with compound nodes spanning multiple trie levels
2. O(k) lookup complexity (key length, not tree size)
3. SIMD-optimized node search for modern CPUs
4. Lower tree height than B+Tree (~2-3 vs ~4 levels for 1M entries)
5. Excellent range query support via leaf links
6. Designed for main-memory/off-heap scenarios

**Implementation effort**: 8-10 weeks

**Alternative**: Option B (B+Tree) is simpler and industry-proven, taking 6-8 weeks. Consider B+Tree if SIMD complexity is a concern or faster time-to-market is needed.

### Complementary Enhancement: Off-Heap Leaf Storage (Appendix B)

**Applicable to any option above.** Moves leaf node payloads to off-heap `MemorySegment` buffers via `LinuxMemorySegmentAllocator`, eliminating per-entry `byte[]` allocations and enabling zero-copy deserialization.

**Why?**
1. Reuses existing allocator infrastructure (`LinuxMemorySegmentAllocator`, `BufferManager`)
2. 60-80% reduction in GC young gen pressure
3. Zero-copy hydration from decompressed segments
4. Same COW/versioning semantics as `KeyValueLeafPage`

**Implementation effort**: 1-2 weeks (can be done in parallel with Option A or B)

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

### Phase 3: HOT Design & Core Nodes (Weeks 6-9)

1. Design `HOTBiNode`, `HOTSpanNode`, `HOTMultiNode`, `HOTLeafNode` classes
2. Implement discriminative bit extraction and partial key logic
3. Create SIMD-optimized child lookup for SpanNode
4. Implement node serialization/deserialization to `MemorySegment`
5. Test COW semantics integration

### Phase 4: HOT Reader & Range Queries (Weeks 10-12)

1. Implement `HOTReader` with page-based traversal
2. Add leaf node sibling links for range iteration
3. Create `HOTRangeIterator` using leaf links
4. Benchmark against B+Tree prototype

### Phase 5: HOT Writer & Node Restructuring (Weeks 13-16)

1. Implement `HOTWriter` with insert/delete
2. Handle node splits: BiNode→SpanNode→MultiNode transitions
3. Implement node merging for deletions
4. Adapt versioning algorithms for HOT nodes
5. Create migration tool from RBTree to HOT format

### Phase 6: Production Rollout (Weeks 17-18)

1. Feature flag for HOT vs RBTree
2. Documentation updates
3. Migration guide for existing databases
4. Performance regression testing

### Optional Phase (Parallel): Off-Heap Leaf Storage (Weeks 1-2)

Can be implemented in parallel with any of the above phases:

1. Add `OffHeapLeafPage` class with packed layout (see Appendix B)
2. Wire `LinuxMemorySegmentAllocator` integration for leaf allocation
3. Update serialization for zero-copy hydration
4. Add guard-based lifetime management
5. Benchmark GC reduction and throughput

---

## Metrics for Success

| Metric | Current | Target (ART) | Target (B+tree) | Target (HOT) | + Off-Heap (Appendix B) |
|--------|---------|--------------|-----------------|--------------|-------------------------|
| Page accesses per lookup | ~20 | ~3-5 (after hydration) | ~4 | **~2-3** | Same |
| Tree height (1M entries) | ~20 | N/A (in-memory) | ~4 | **~2-3** | Same |
| Range query efficiency | O(k × log n) | O(k + m) | O(log n + m) | O(k + m) | Same |
| Memory overhead | Low | +50-80 MB for 1M entries | Similar to current | Similar | Off-heap, GC-free |
| Insert latency | O(log n) page accesses | O(log n) + O(k) | O(log n / 64) | O(k) | Same |
| Cache hit rate | ~30-40% | ~80-90% | ~85-95% | **~90-98%** | Same |
| SIMD utilization | None | None | Limited | **Designed for SIMD** | Same |
| `byte[]` per leaf | 1 per entry | 1 per entry | 1 per entry | 1 per entry | **0** |
| GC pressure | High | Medium | Medium | Medium | **-60% to -80%** |
| Hydration copies | Per-entry | Per-entry | Per-entry | Per-entry | **Zero-copy** |

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

1. "The Adaptive Radix Tree: ARTful Indexing for Main-Memory Databases" - Viktor Leis et al., ICDE 2013
2. "Modern B-Tree Techniques" - Goetz Graefe, Foundations and Trends in Databases, 2011
3. **"HOT: A Height Optimized Trie Index for Main-Memory Database Systems"** - Robert Binna et al., SIGMOD 2018
4. Sirix existing ART implementation: `io.sirix.index.art.AdaptiveRadixTree`
5. Sirix RBTree implementation: `io.sirix.index.redblacktree.RBTreeReader/Writer`
6. Sirix off-heap allocator: `io.sirix.cache.LinuxMemorySegmentAllocator`

---

## Appendix B: Off-Heap Leaf Storage via MemorySegment

This enhancement can be applied to **any of the three options above** (ART, B+Tree, or Sorted Arrays). The goal is to eliminate on-heap `byte[]` allocations for leaf node payloads by storing them in off-heap `MemorySegment` buffers, leveraging the existing `LinuxMemorySegmentAllocator` infrastructure.

### Motivation

| Problem | Impact |
|---------|--------|
| Per-entry `byte[]` allocations during deserialization | High GC pressure, 500-1000 allocations per page |
| On-heap leaf storage | Competes with application heap, G1 marking overhead |
| Copy-heavy hydration path | CPU cycles wasted copying decompressed bytes |

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       OFF-HEAP LEAF STORAGE MODEL                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ALLOCATOR LAYER                    LEAF PAGE LAYER                        │
│   ┌─────────────────────────┐       ┌─────────────────────────────────────┐ │
│   │ LinuxMemorySegmentAlloc │       │  OffHeapLeafPage                    │ │
│   │ ├── 4KB size class      │──────>│  ├── MemorySegment buffer (off-heap)│ │
│   │ ├── 8KB size class      │       │  ├── Releaser (returns to pool)    │ │
│   │ ├── 16KB size class     │       │  ├── int entryCount (on-heap)      │ │
│   │ ├── 32KB size class     │       │  ├── int usedBytes (on-heap)       │ │
│   │ ├── 64KB size class  ◄──┘       │  └── guard count (on-heap)         │ │
│   │ ├── 128KB size class    │       └─────────────────────────────────────┘ │
│   │ └── 256KB size class    │                                               │
│   └─────────────────────────┘                                               │
│                                                                             │
│   ON CLOSE: releaser.run() → segment returns to allocator pool             │
│   ON COW:   allocate new segment → copy active region → mutate             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Off-Heap Leaf Node Layout

Each leaf node gets a fixed-size segment from the allocator's size class. The packed layout (little-endian) is designed for zero-copy reads:

```
Offset 0:
┌────────────────────────────────────────────────────────────────────────────┐
│ Header (24 bytes, fixed)                                                   │
├────────────────────────────────────────────────────────────────────────────┤
│ [u8  nodeKind=LEAF]                     - Node type discriminator          │
│ [u8  flags]                             - Dirty, overflow, etc.            │
│ [u16 entryCount]                        - Number of key-value pairs        │
│ [u16 prefixLen]                         - Path compression prefix length   │
│ [u16 reserved]                          - Alignment padding                │
│ [u32 nextLeafKey]                       - Sibling link for range scans     │
│ [u32 prevLeafKey]                       - Reverse sibling link             │
│ [u32 keyAreaEnd]                        - Keys grow from header downward   │
│ [u32 valAreaStart]                      - Values grow from end upward      │
├────────────────────────────────────────────────────────────────────────────┤
│ Entry Index (4 bytes × entryCount, sorted by key)                          │
├────────────────────────────────────────────────────────────────────────────┤
│ [u32 entryOffset[0]]  → points to key record 0                             │
│ [u32 entryOffset[1]]  → points to key record 1                             │
│ ...                                                                        │
│ [u32 entryOffset[n-1]] → points to key record n-1                          │
├────────────────────────────────────────────────────────────────────────────┤
│ Key Area (grows downward from header)                                      │
├────────────────────────────────────────────────────────────────────────────┤
│ Key Record Layout:                                                         │
│   [u16 keyLen][key bytes...][u32 valueOffset]                              │
│                                         │                                  │
│                                         ▼                                  │
├────────────────────────────────────────────────────────────────────────────┤
│ Value Area (grows upward from segment end)                                 │
├────────────────────────────────────────────────────────────────────────────┤
│ Value Record Layout:                                                       │
│   [u16 valueLen][value bytes...]                                           │
│   (e.g., NodeReferences serialized bytes)                                  │
└────────────────────────────────────────────────────────────────────────────┘
```

**Design rationale:**
- Entry index enables binary search (O(log n) within page)
- Keys and values in separate areas → better cache locality for key-only scans
- Grows from opposite ends → simple compaction, no fragmentation until full
- 4-byte alignment for all offsets → matches `MemorySegment.get(ValueLayout.JAVA_INT, ...)`

### Java Implementation

```java
package io.sirix.index.offheap;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.Runnable;

/**
 * Off-heap leaf page for secondary indexes.
 * Stores key-value pairs in a MemorySegment allocated from LinuxMemorySegmentAllocator.
 */
public final class OffHeapLeafPage implements AutoCloseable {
    
    // Header layout constants
    private static final long OFFSET_NODE_KIND = 0;
    private static final long OFFSET_FLAGS = 1;
    private static final long OFFSET_ENTRY_COUNT = 2;
    private static final long OFFSET_PREFIX_LEN = 4;
    private static final long OFFSET_NEXT_LEAF = 8;
    private static final long OFFSET_PREV_LEAF = 12;
    private static final long OFFSET_KEY_AREA_END = 16;
    private static final long OFFSET_VAL_AREA_START = 20;
    private static final int HEADER_SIZE = 24;
    
    private static final int ENTRY_INDEX_ENTRY_SIZE = 4; // u32 offset per entry
    
    private final MemorySegment segment;
    private final Runnable releaser;
    private final int capacity; // segment.byteSize()
    
    // On-heap metadata (minimal footprint)
    private volatile boolean closed = false;
    private final java.util.concurrent.atomic.AtomicInteger guardCount = 
        new java.util.concurrent.atomic.AtomicInteger(0);
    
    /**
     * Create from pre-allocated segment (from allocator or deserialization).
     */
    public OffHeapLeafPage(MemorySegment segment, Runnable releaser) {
        if (segment == null || !segment.isNative()) {
            throw new IllegalArgumentException("Segment must be native (off-heap)");
        }
        this.segment = segment;
        this.releaser = releaser;
        this.capacity = (int) segment.byteSize();
    }
    
    /**
     * Initialize as empty leaf page.
     */
    public void initEmpty(int nodeKind, long nextLeafKey, long prevLeafKey) {
        segment.set(ValueLayout.JAVA_BYTE, OFFSET_NODE_KIND, (byte) nodeKind);
        segment.set(ValueLayout.JAVA_BYTE, OFFSET_FLAGS, (byte) 0);
        segment.set(ValueLayout.JAVA_SHORT, OFFSET_ENTRY_COUNT, (short) 0);
        segment.set(ValueLayout.JAVA_SHORT, OFFSET_PREFIX_LEN, (short) 0);
        segment.set(ValueLayout.JAVA_INT, OFFSET_NEXT_LEAF, (int) nextLeafKey);
        segment.set(ValueLayout.JAVA_INT, OFFSET_PREV_LEAF, (int) prevLeafKey);
        segment.set(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END, HEADER_SIZE);
        segment.set(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START, capacity);
    }
    
    // ============= Accessors =============
    
    public int getEntryCount() {
        return Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, OFFSET_ENTRY_COUNT));
    }
    
    public long getNextLeafKey() {
        return Integer.toUnsignedLong(segment.get(ValueLayout.JAVA_INT, OFFSET_NEXT_LEAF));
    }
    
    public long getPrevLeafKey() {
        return Integer.toUnsignedLong(segment.get(ValueLayout.JAVA_INT, OFFSET_PREV_LEAF));
    }
    
    /**
     * Zero-copy slice for reading key at given entry index.
     */
    public MemorySegment getKeySlice(int entryIndex) {
        int entryOffset = getEntryOffset(entryIndex);
        int keyLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, entryOffset));
        return segment.asSlice(entryOffset + 2, keyLen);
    }
    
    /**
     * Zero-copy slice for reading value at given entry index.
     */
    public MemorySegment getValueSlice(int entryIndex) {
        int entryOffset = getEntryOffset(entryIndex);
        int keyLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, entryOffset));
        int valueOffset = segment.get(ValueLayout.JAVA_INT, entryOffset + 2 + keyLen);
        int valueLen = Short.toUnsignedInt(segment.get(ValueLayout.JAVA_SHORT, valueOffset));
        return segment.asSlice(valueOffset + 2, valueLen);
    }
    
    private int getEntryOffset(int entryIndex) {
        long indexOffset = HEADER_SIZE + (long) entryIndex * ENTRY_INDEX_ENTRY_SIZE;
        return segment.get(ValueLayout.JAVA_INT, indexOffset);
    }
    
    // ============= Mutations =============
    
    /**
     * Insert key-value pair, maintaining sorted order.
     * @return true if page needs split (no space left)
     */
    public boolean insert(MemorySegment key, MemorySegment value) {
        int entryCount = getEntryCount();
        int keyAreaEnd = segment.get(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END);
        int valAreaStart = segment.get(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START);
        
        int keyLen = (int) key.byteSize();
        int valLen = (int) value.byteSize();
        int keyRecordSize = 2 + keyLen + 4; // u16 len + key + u32 valueOffset
        int valRecordSize = 2 + valLen;     // u16 len + value
        int indexEntrySize = 4;             // u32 entry offset
        
        int spaceNeeded = keyRecordSize + valRecordSize + indexEntrySize;
        int freeSpace = valAreaStart - keyAreaEnd - (entryCount * ENTRY_INDEX_ENTRY_SIZE);
        
        if (spaceNeeded > freeSpace) {
            return true; // Signal split needed
        }
        
        // Find insertion position via binary search
        int insertPos = binarySearchForInsert(key);
        
        // Allocate value space (grows upward from end)
        int newValOffset = valAreaStart - valRecordSize;
        segment.set(ValueLayout.JAVA_SHORT, newValOffset, (short) valLen);
        MemorySegment.copy(value, 0, segment, newValOffset + 2, valLen);
        segment.set(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START, newValOffset);
        
        // Allocate key record (grows downward from header)
        int newKeyOffset = keyAreaEnd;
        segment.set(ValueLayout.JAVA_SHORT, newKeyOffset, (short) keyLen);
        MemorySegment.copy(key, 0, segment, newKeyOffset + 2, keyLen);
        segment.set(ValueLayout.JAVA_INT, newKeyOffset + 2 + keyLen, newValOffset);
        segment.set(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END, newKeyOffset + keyRecordSize);
        
        // Shift entry index to make room
        long indexBase = HEADER_SIZE;
        if (insertPos < entryCount) {
            long srcOffset = indexBase + (long) insertPos * ENTRY_INDEX_ENTRY_SIZE;
            long dstOffset = srcOffset + ENTRY_INDEX_ENTRY_SIZE;
            long shiftBytes = (long) (entryCount - insertPos) * ENTRY_INDEX_ENTRY_SIZE;
            MemorySegment.copy(segment, srcOffset, segment, dstOffset, shiftBytes);
        }
        
        // Write new entry index
        segment.set(ValueLayout.JAVA_INT, indexBase + (long) insertPos * ENTRY_INDEX_ENTRY_SIZE, newKeyOffset);
        
        // Increment entry count
        segment.set(ValueLayout.JAVA_SHORT, OFFSET_ENTRY_COUNT, (short) (entryCount + 1));
        
        return false;
    }
    
    private int binarySearchForInsert(MemorySegment targetKey) {
        int low = 0;
        int high = getEntryCount() - 1;
        
        while (low <= high) {
            int mid = (low + high) >>> 1;
            MemorySegment midKey = getKeySlice(mid);
            int cmp = compareKeys(midKey, targetKey);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Exact match
            }
        }
        return low; // Insertion point
    }
    
    private int compareKeys(MemorySegment a, MemorySegment b) {
        long lenA = a.byteSize();
        long lenB = b.byteSize();
        long minLen = Math.min(lenA, lenB);
        
        for (long i = 0; i < minLen; i++) {
            int cmp = Byte.compareUnsigned(
                a.get(ValueLayout.JAVA_BYTE, i),
                b.get(ValueLayout.JAVA_BYTE, i)
            );
            if (cmp != 0) return cmp;
        }
        return Long.compare(lenA, lenB);
    }
    
    // ============= Lifecycle =============
    
    public int acquireGuard() {
        return guardCount.incrementAndGet();
    }
    
    public int releaseGuard() {
        return guardCount.decrementAndGet();
    }
    
    public int getGuardCount() {
        return guardCount.get();
    }
    
    public boolean isClosed() {
        return closed;
    }
    
    public MemorySegment getSegment() {
        return segment;
    }
    
    public int getUsedBytes() {
        int keyAreaEnd = segment.get(ValueLayout.JAVA_INT, OFFSET_KEY_AREA_END);
        int valAreaStart = segment.get(ValueLayout.JAVA_INT, OFFSET_VAL_AREA_START);
        return keyAreaEnd + (capacity - valAreaStart);
    }
    
    @Override
    public void close() {
        if (closed) return;
        
        int guards = guardCount.get();
        if (guards != 0) {
            throw new IllegalStateException(
                "Cannot close OffHeapLeafPage with " + guards + " active guards"
            );
        }
        
        closed = true;
        if (releaser != null) {
            releaser.run(); // Return segment to allocator pool
        }
    }
}
```

### Serialization & Zero-Copy Hydration

```java
/**
 * Serializer for OffHeapLeafPage - streams raw segment bytes for zero-copy.
 */
public class OffHeapLeafPageSerializer {
    
    /**
     * Serialize: write raw segment bytes (no transformation).
     */
    public void serialize(BytesOut sink, OffHeapLeafPage page) {
        int usedBytes = page.getUsedBytes();
        MemorySegment seg = page.getSegment();
        
        // Write used size first (for allocation on deserialize)
        sink.writeInt(usedBytes);
        
        // Write header + key area (contiguous from start)
        int keyAreaEnd = seg.get(ValueLayout.JAVA_INT, OffHeapLeafPage.OFFSET_KEY_AREA_END);
        for (int i = 0; i < keyAreaEnd; i++) {
            sink.writeByte(seg.get(ValueLayout.JAVA_BYTE, i));
        }
        
        // Write value area (from valAreaStart to end)
        int valAreaStart = seg.get(ValueLayout.JAVA_INT, OffHeapLeafPage.OFFSET_VAL_AREA_START);
        int capacity = (int) seg.byteSize();
        for (int i = valAreaStart; i < capacity; i++) {
            sink.writeByte(seg.get(ValueLayout.JAVA_BYTE, i));
        }
    }
    
    /**
     * Deserialize: zero-copy hydration from decompressed MemorySegment.
     */
    public OffHeapLeafPage deserializeZeroCopy(
        MemorySegment decompressedSource,
        long offset,
        LinuxMemorySegmentAllocator allocator
    ) {
        int usedBytes = decompressedSource.get(ValueLayout.JAVA_INT, offset);
        offset += 4;
        
        // Allocate from pool (picks appropriate size class)
        var allocation = allocator.allocate(usedBytes);
        MemorySegment target = allocation.segment();
        
        // Single bulk copy from decompressed source to off-heap target
        MemorySegment.copy(decompressedSource, offset, target, 0, usedBytes);
        
        return new OffHeapLeafPage(target, allocation.releaser());
    }
}
```

### Integration with Existing Options

This off-heap storage can be combined with any of the three main options:

| Option | Off-Heap Integration |
|--------|---------------------|
| **A: ART** | ART leaf nodes (`LeafNode<K,V>`) store values in `OffHeapLeafPage` segments |
| **B: B+Tree** | `BPlusLeafNode` stores its key-value arrays in a single `OffHeapLeafPage` |
| **C: Sorted Arrays** | Each `SortedArrayIndex` page backed by an `OffHeapLeafPage` segment |

### COW (Copy-on-Write) Handling

```java
/**
 * Copy-on-write for off-heap leaf pages.
 */
public OffHeapLeafPage copyOnWrite(OffHeapLeafPage original, LinuxMemorySegmentAllocator allocator) {
    int usedBytes = original.getUsedBytes();
    
    // Allocate fresh segment from pool
    var allocation = allocator.allocate(usedBytes);
    MemorySegment newSeg = allocation.segment();
    
    // Copy active region only (header + keys + values)
    MemorySegment origSeg = original.getSegment();
    int keyAreaEnd = origSeg.get(ValueLayout.JAVA_INT, OffHeapLeafPage.OFFSET_KEY_AREA_END);
    int valAreaStart = origSeg.get(ValueLayout.JAVA_INT, OffHeapLeafPage.OFFSET_VAL_AREA_START);
    int capacity = (int) origSeg.byteSize();
    
    // Copy header + key area
    MemorySegment.copy(origSeg, 0, newSeg, 0, keyAreaEnd);
    
    // Copy value area
    int valLen = capacity - valAreaStart;
    MemorySegment.copy(origSeg, valAreaStart, newSeg, valAreaStart, valLen);
    
    return new OffHeapLeafPage(newSeg, allocation.releaser());
}
```

### Cache Integration

```java
// Weight calculation for cache eviction
public long weightOf(OffHeapLeafPage page) {
    return page.getSegment().byteSize(); // Exact off-heap bytes consumed
}

// Guard-based pinning (same pattern as KeyValueLeafPage)
public class OffHeapLeafPageGuard implements AutoCloseable {
    private final OffHeapLeafPage page;
    private boolean released = false;
    
    public OffHeapLeafPageGuard(OffHeapLeafPage page) {
        this.page = page;
        page.acquireGuard();
    }
    
    @Override
    public void close() {
        if (!released) {
            released = true;
            page.releaseGuard();
        }
    }
}
```

### Migration Steps

1. **Add `OffHeapLeafPage` class** with layout and accessors as shown above
2. **Wire allocator plumbing**: Request/release via `LinuxMemorySegmentAllocator` in leaf factory
3. **Update index reader/writer** to operate on `MemorySegment`-backed leaves
4. **Adjust serialization** to stream raw segment bytes; hydration maps decompressed `MemorySegment` directly
5. **Update caches** to weight by `segment.byteSize()` and ensure guards manage lifetime
6. **Tests**:
   - Unit: insert/update/delete, split/merge, range scan across sibling links
   - Persistence: serialize→deserialize→validate byte-for-byte equality
   - Stress: allocator exhaustion, repeated COW churn, parallel reads with guards

### Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Use-after-free | Guard counts gate `releaser.run()`; assertions in `close()` |
| Fragmentation | Rely on allocator size classes; compact only on leaf rebuild |
| Alignment errors | Centralize offset math helpers; fuzz with random key/value lengths |
| GC visibility | Keep on-heap metadata minimal (counts/offsets only) |

### Expected Impact

| Metric | Before (on-heap) | After (off-heap) |
|--------|------------------|------------------|
| `byte[]` per leaf | 1 per entry | **0** |
| Hydration copies | Per-entry copy | **Bulk copy or zero-copy slice** |
| GC young gen pressure | High | **-60% to -80%** |
| Cache locality | Poor (scattered objects) | **Excellent (contiguous segment)** |

### Success Criteria

- No on-heap `byte[]` for leaf storage in steady state
- Hydration path is zero-copy from decompressed `MemorySegment`
- Benchmarks show lower allocation rate and stable throughput vs. on-heap baseline

