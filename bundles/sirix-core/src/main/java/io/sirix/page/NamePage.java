/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.page;

import io.sirix.utils.NamePageHash;
import io.sirix.utils.ToStringHelper;
import io.sirix.access.DatabaseType;
import io.sirix.node.FsstSymbolTableNode;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.TreeSet;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.Cache;
import io.sirix.cache.GlobalDictionaryRecordCacheKey;
import io.sirix.cache.NamesCacheKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.name.Names;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

/**
 * The name page holds all names and their keys for a revision. Furthermore, it has references to
 * name indexes.
 */
public final class NamePage extends AbstractForwardingPage {

  /**
   * Offset of reference to attributes index-tree.
   */
  public static final int ATTRIBUTES_REFERENCE_OFFSET = 0;

  /**
   * Offset of reference to elements index-tree.
   */
  public static final int ELEMENTS_REFERENCE_OFFSET = 1;

  /**
   * Offset of reference to namespace index-tree.
   */
  public static final int NAMESPACE_REFERENCE_OFFSET = 2;

  /**
   * Offset of reference to processing instruction index-tree.
   */
  public static final int PROCESSING_INSTRUCTION_REFERENCE_OFFSET = 3;

  /**
   * Offset of reference to the JSON object-KEY dictionary — every object member NAME in the resource,
   * in one dictionary.
   *
   * <p>
   * One dictionary for all six fused {@code OBJECT_NAMED_*} kinds, and that is not a simplification:
   * the kind records what a member's VALUE is ({@code OBJECT_NAMED_STRING} is a member whose value is
   * a string), while what is stored here is the member's name, which is an object key whatever the
   * value turns out to be. {@code setName} writes all six here and {@code getName}/{@code getRawName}
   * read all six back from here, so a member that changes value type keeps its name key.
   *
   * <p>
   * Shares offset 0 with {@link #ATTRIBUTES_REFERENCE_OFFSET}: a resource is either JSON or XML, so
   * within one resource the offset denotes exactly one dictionary.
   */
  public static final int JSON_OBJECT_KEY_REFERENCE_OFFSET = 0;

  /**
   * Offset of reference to the FSST symbol-table tree in a JSON resource.
   *
   * <p>
   * Symbol tables are strings-about-strings, and they need exactly what this page already provides: a
   * copy-on-write versioned sub-trie whose entries are individual records. Putting them here rather
   * than behind a new {@link io.sirix.index.IndexType} and a new {@link RevisionRootPage} slot
   * matters — the revision root's reference count is the on-disk contract, with no per-page count
   * written alongside it, so growing it is a wire-format change. A new offset inside this page is
   * not: the delegate promotes itself from {@link ReferencesPage4} to {@link BitmapReferencesPage} on
   * demand, and a slot that is never written costs a resource nothing.
   *
   * <p>
   * <b>Why the offset differs by database type.</b> The offsets are per-type namespaces already —
   * {@link #JSON_OBJECT_KEY_REFERENCE_OFFSET} and {@link #ATTRIBUTES_REFERENCE_OFFSET} are both 0,
   * because a resource is either JSON or XML and never both. That is not merely tidy here, it is
   * required: this page's keyed-trie dictionary counters are serialized <em>positionally</em> (a
   * count, followed by one value per offset from 0 upwards), so the dictionary offsets in use must
   * form a gapless run. Secondary HOT allocator metadata is a separate sparse map. A JSON
   * resource occupies only offset 0, so its symbol tables go at 1; an XML resource occupies 0-3, so
   * its symbol tables go at 4. Picking one constant for both would leave a JSON resource holding {0,
   * 4} and the serializer would write offsets 0 and 1 — losing the symbol tables' bookkeeping and
   * making every stored table unreachable after a reload. {@code PageKind.NAMEPAGE} now rejects a
   * gapped map outright rather than writing it.
   */
  public static final int JSON_FSST_SYMBOL_TABLE_REFERENCE_OFFSET = 1;

  /**
   * Offset of reference to the FSST symbol-table tree in an XML resource, past the four name
   * dictionaries XML occupies. See {@link #JSON_FSST_SYMBOL_TABLE_REFERENCE_OFFSET}.
   */
  public static final int XML_FSST_SYMBOL_TABLE_REFERENCE_OFFSET = 4;

  /**
   * The offset the FSST symbol-table tree lives at for a given database type.
   *
   * @param databaseType the database type
   * @return the dictionary offset to use
   */
  public static int fsstSymbolTableOffset(final DatabaseType databaseType) {
    return switch (databaseType) {
      case JSON -> JSON_FSST_SYMBOL_TABLE_REFERENCE_OFFSET;
      case XML -> XML_FSST_SYMBOL_TABLE_REFERENCE_OFFSET;
    };
  }

  /**
   * Offset of reference to the global projection VALUE dictionary in a JSON resource — the sub-trie
   * holding {@code id -> value} records and their forward directory for every high-cardinality string
   * column of every projection index on the resource.
   *
   * <p>
   * Third use of the extension pattern {@link #JSON_FSST_SYMBOL_TABLE_REFERENCE_OFFSET} documents,
   * and for the same reason: what is stored is copy-on-write versioned state made of individual
   * records, which is precisely what this page's sub-tries are. Row cells in a projection's row
   * groups refer to values by id, so an id must keep meaning the same thing in every revision that
   * can still be read — which CoW gives for free and a side blob does not.
   *
   * <p>
   * <b>One sub-trie, not one per column.</b> The offsets are a scarce, wire-format-adjacent resource
   * that must form a gapless run per database type, and a projection's column set is not known at
   * bootstrap and grows with every index definition — so one offset per column is not merely
   * wasteful, it is unimplementable. The node-key space inside the single sub-trie is partitioned
   * into per-column namespaces instead; see {@code io.sirix.index.projection.GlobalValueDictionary}
   * for the key layout.
   *
   * <p>
   * JSON occupies {0, 1}, so this is 2.
   */
  public static final int JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET = 2;

  /**
   * Offset of reference to the global projection value dictionary in an XML resource, past the four
   * name dictionaries and the symbol-table tree. See
   * {@link #JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET}.
   *
   * <p>
   * Defined for symmetry and to keep the per-type offset run coherent. Projection indexes are
   * declared over JSON resources only today, so nothing writes here.
   */
  public static final int XML_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET = 5;

  /**
   * The offset the global projection value dictionary lives at for a given database type.
   *
   * @param databaseType the database type
   * @return the dictionary offset to use
   */
  public static int projectionValueDictionaryOffset(final DatabaseType databaseType) {
    return switch (databaseType) {
      case JSON -> JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET;
      case XML -> XML_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET;
    };
  }

  /**
   * Determine whether a physical slot belongs to the keyed-trie dictionary prefix of this page.
   *
   * <p>The prefix is {@code [0, 3)} for JSON and {@code [0, 6)} for XML. Every slot at or above
   * that boundary is a secondary NAME index and is therefore addressed exclusively through HOT.
   * Keeping this check here makes the page that owns the slot layout the sole authority for the
   * boundary.</p>
   *
   * @param databaseType database type defining the reserved prefix
   * @param index physical NamePage slot
   * @return {@code true} for a reserved dictionary slot, {@code false} for a secondary NAME slot
   * @throws NullPointerException if {@code databaseType} is {@code null}
   * @throws IllegalArgumentException if {@code index} is outside the NamePage reference space
   */
  public static boolean isNameDictionarySlot(final DatabaseType databaseType, final int index) {
    final int firstSecondaryIndex = firstSecondaryNameIndex(databaseType);
    if (index < 0 || index >= Constants.INP_REFERENCE_COUNT) {
      throw new IllegalArgumentException("NamePage slot outside reference space: " + index);
    }
    return index < firstSecondaryIndex;
  }

  private static int firstSecondaryNameIndex(final DatabaseType databaseType) {
    Objects.requireNonNull(databaseType, "databaseType must not be null");
    return switch (databaseType) {
      case JSON -> PageConstants.JSON_NAME_INDEX_OFFSET;
      case XML -> PageConstants.XML_NAME_INDEX_OFFSET;
    };
  }

  /** {@link #dictionaryOffset}'s answer for a kind whose name does not come from a dictionary. */
  public static final int NO_DICTIONARY = -1;

  /**
   * The dictionary offset a node kind's names live at, or {@link #NO_DICTIONARY}.
   *
   * <p>
   * Exists because a caller outside this page needs the offset to build a
   * {@link io.sirix.cache.NamesCacheKey} without first resolving a {@code NamePage} — which is the
   * whole point, since resolving one costs a page read.
   *
   * <p>
   * It answers {@link #NO_DICTIONARY} rather than throwing for everything else, and that is
   * load-bearing rather than defensive: {@link #getName} answers some kinds WITHOUT consulting any
   * dictionary at all ({@code ARRAY} and {@code OBJECT} yield the synthetic {@code __array__} /
   * {@code __object__} literals the path summary uses), and {@link #getRawName} does not accept those
   * kinds at all. So the three are deliberately not the same set, and a caller using this to take a
   * shortcut must fall back to {@code getName}/{@code getRawName} whenever the answer is
   * {@code NO_DICTIONARY} — which keeps this page the authority for every kind it does not name here,
   * and makes a future kind added there but forgotten here merely slow, never wrong.
   *
   * <p>
   * Note that {@code ATTRIBUTES} and {@code JSON_OBJECT_KEY} share offset 0: a resource is either XML
   * or JSON, so within one resource the offset denotes exactly one dictionary.
   *
   * @param nodeKind the kind whose names are wanted
   * @return the dictionary offset, or {@link #NO_DICTIONARY}
   */
  public static int dictionaryOffset(final NodeKind nodeKind) {
    // $CASES-OMITTED$
    return switch (nodeKind) {
      case ELEMENT -> ELEMENTS_REFERENCE_OFFSET;
      case NAMESPACE -> NAMESPACE_REFERENCE_OFFSET;
      case ATTRIBUTE -> ATTRIBUTES_REFERENCE_OFFSET;
      case PROCESSING_INSTRUCTION -> PROCESSING_INSTRUCTION_REFERENCE_OFFSET;
      // All six fused kinds, one dictionary: the kind says what the member's VALUE is, while what
      // is stored is the member's NAME, which is an object key regardless. Mirrors setName and
      // getName/getRawName, which write and read all six through jsonObjectKeys.
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL ->
        JSON_OBJECT_KEY_REFERENCE_OFFSET;
      default -> NO_DICTIONARY;
    };
  }

  /**
   * Attribute names.
   */
  private Names attributes;

  /**
   * Element names.
   */
  private Names elements;

  /**
   * Namespace URIs.
   */
  private Names namespaces;

  /**
   * Processing instruction names.
   */
  private Names processingInstructions;

  /**
   * JSON Object key names.
   */
  private Names jsonObjectKeys;

  /**
   * The references page delegate instance.
   */
  private Page delegate;

  /**
   * The number of arrays stored.
   */
  private final int numberOfArrays;


  /**
   * Maximum node keys.
   */
  private final Int2LongMap maxNodeKeys;

  /**
   * Maximum HOT page keys per index number. Used by the canonical HOT index writer for persistent page key allocation
   * across transactions.
   */
  private final Int2LongMap maxHotPageKeys;

  /**
   * Current maximum levels of indirect pages in the tree.
   */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Live {@link io.sirix.node.HashEntryNode} node-keys per dictionary offset (Approach B,
   * docs/NAME_DICTIONARY_RECONSTRUCTION_PLAN.md). Persisted so a later revision reconstructs each
   * dictionary in O(live) rather than scanning {@code 1..maxNodeKey} over the churn-inflated slot
   * range. Carried forward across CoW; for a dictionary loaded (and possibly mutated) this
   * transaction the authoritative set is re-derived from its {@link Names} at serialize time.
   */
  private final Int2ObjectMap<Roaring64Bitmap> liveEntryNodeKeys;

  /**
   * Whether this page is a private write copy and must never adopt a mutable shared Names-cache
   * value, even when a read-only reader is used for its first dictionary lookup.
   */
  private final boolean writeCopy;

  /**
   * Create name page.
   */
  public NamePage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    maxHotPageKeys = new Int2LongOpenHashMap();
    attributes = Names.getInstance(ATTRIBUTES_REFERENCE_OFFSET);
    elements = Names.getInstance(ELEMENTS_REFERENCE_OFFSET);
    namespaces = Names.getInstance(NAMESPACE_REFERENCE_OFFSET);
    processingInstructions = Names.getInstance(PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
    jsonObjectKeys = Names.getInstance(JSON_OBJECT_KEY_REFERENCE_OFFSET);
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
    liveEntryNodeKeys = new Int2ObjectOpenHashMap<>();
    numberOfArrays = 0;
    writeCopy = false;
  }

  /**
   * Constructor when is deserialized data
   *
   * @param delegate The references page delegate instance.
   * @param maxNodeKeys Maximum node keys.
   * @param currentMaxLevelsOfIndirectPages Current maximum levels of indirect pages in the tree.
   * @param numberOfArrays The number of arrays stored.
   */
  NamePage(final Page delegate, final Int2LongMap maxNodeKeys, final Int2IntMap currentMaxLevelsOfIndirectPages,
      final int numberOfArrays) {
    this(delegate, maxNodeKeys, new Int2LongOpenHashMap(), currentMaxLevelsOfIndirectPages, numberOfArrays);
  }

  /**
   * Constructor when is deserialized data (with HOT page keys).
   */
  NamePage(final Page delegate, final Int2LongMap maxNodeKeys, final Int2LongMap maxHotPageKeys,
      final Int2IntMap currentMaxLevelsOfIndirectPages, final int numberOfArrays) {
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.maxHotPageKeys = maxHotPageKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
    this.liveEntryNodeKeys = new Int2ObjectOpenHashMap<>();
    this.numberOfArrays = numberOfArrays;
    this.writeCopy = false;
  }

  /**
   * Make a private write-side copy of an immutable, persisted historical page.
   *
   * <p>The copy is complete before this method returns: every structural reference, allocator map
   * and live-entry bitmap is detached from {@code historicalPage}. A caller can therefore publish
   * the result to its transaction-intent log only after this method succeeds, without any mutation
   * having leaked into the historical page while the copy was being built.</p>
   *
   * <p>{@link Names} dictionaries are deliberately <em>not</em> copied or loaded here. They are
   * mutable, potentially large derived views of the records below this page. Sharing a loaded
   * instance would let a writer change a historical revision; eagerly cloning every loaded
   * dictionary would make an unrelated secondary-index mutation pay for all name maps. Instead the
   * returned page starts with no materialized dictionaries. Its existing read and write methods
   * reconstruct only the first dictionary they actually touch, using the reader/writer supplied to
   * that operation. The reconstructed {@code Names} belongs solely to the returned page.</p>
   *
   * <p>The source must be a persisted historical page, not a page with unpublished dictionary
   * mutations in the current transaction. Persisted records and the copied high-water/live-key
   * metadata are the authoritative state from which a dictionary is reconstructed.</p>
   *
   * @param historicalPage immutable persisted page to detach
   * @return a fully detached page suitable for mutation and subsequent intent-log publication
   * @throws NullPointerException if {@code historicalPage} is {@code null}
   */
  public static NamePage copyForWrite(final NamePage historicalPage) {
    return new NamePage(Objects.requireNonNull(historicalPage, "historicalPage must not be null"));
  }

  /**
   * Compatibility constructor with the detached/lazy semantics of
   * {@link #copyForWrite(NamePage)}.
   *
   * @param other immutable persisted page to detach
   * @deprecated use {@link #copyForWrite(NamePage)} to make the historical-page precondition
   *             explicit at the call site
   */
  @Deprecated
  public NamePage(final NamePage other) {
    final Page otherDelegate = Objects.requireNonNull(other, "other must not be null").delegate;
    if (otherDelegate instanceof ReferencesPage4 ref) {
      this.delegate = new ReferencesPage4(ref);
    } else if (otherDelegate instanceof BitmapReferencesPage bmp) {
      this.delegate = new BitmapReferencesPage(otherDelegate, bmp.getBitmap());
    } else if (otherDelegate instanceof FullReferencesPage full) {
      this.delegate = new FullReferencesPage(full);
    } else {
      throw new IllegalStateException(
          "Unknown NamePage delegate type, cannot clone: " + otherDelegate.getClass().getName());
    }
    this.maxNodeKeys = new Int2LongOpenHashMap(other.maxNodeKeys);
    this.maxHotPageKeys = new Int2LongOpenHashMap(other.maxHotPageKeys);
    this.currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap(other.currentMaxLevelsOfIndirectPages);
    // Deep-copy the live-key bitmaps so a CoW write never mutates the historical revision's set.
    this.liveEntryNodeKeys = new Int2ObjectOpenHashMap<>(other.liveEntryNodeKeys.size());
    for (final var entry : other.liveEntryNodeKeys.int2ObjectEntrySet()) {
      this.liveEntryNodeKeys.put(entry.getIntKey(), entry.getValue().clone());
    }
    this.numberOfArrays = other.numberOfArrays;
    this.writeCopy = true;
    // Names are derived, mutable caches. Leaving them null makes materialization lazy and private to
    // this page; neither the source nor any of its already-loaded dictionaries is touched or shared.
    this.attributes = null;
    this.elements = null;
    this.namespaces = null;
    this.processingInstructions = null;
    this.jsonObjectKeys = null;
  }

  /**
   * Compatibility constructor for callers migrating to {@link #copyForWrite(NamePage)}.
   *
   * <p>The reader is intentionally unused: copying must never perform IO or populate the source's
   * lazy dictionaries. The resulting page has exactly the same detached/lazy semantics as
   * {@code copyForWrite(other)}.</p>
   *
   * @param other immutable persisted page to detach
   * @param storageEngineReader ignored; retained temporarily for source compatibility
   * @deprecated use {@link #copyForWrite(NamePage)}
   */
  @Deprecated
  public NamePage(final NamePage other, final StorageEngineReader storageEngineReader) {
    this(Objects.requireNonNull(other, "other must not be null"));
  }


  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key
   */
  public byte[] getRawName(final int key, final NodeKind nodeKind, final StorageEngineReader storageEngineReader) {
    final byte[] rawName;
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        rawName = elements.getRawName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        rawName = namespaces.getRawName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        rawName = attributes.getRawName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        rawName = processingInstructions.getRawName(key);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        rawName = jsonObjectKeys.getRawName(key);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
    return rawName;
  }

  private Names getNames(StorageEngineReader storageEngineReader, int offset) {
    final var maxNodeKey = maxNodeKeys.getOrDefault(offset, 0L);
    // Persisted live entry node-keys -> O(live) reconstruction; null falls back to the scan.
    final Roaring64Bitmap liveKeys = liveEntryNodeKeys.get(offset);
    // A detached write page must own every mutable Names instance it materializes. It may be read
    // through a read-only delegate before its first name mutation, so hasTrxIntentLog() alone is not
    // a sufficient ownership test: adopting the shared NamesCache value here would let that later
    // mutation alter historical readers. Reconstructing is paid once per touched dictionary.
    if (writeCopy || storageEngineReader.hasTrxIntentLog()) {
      return Names.fromStorage(storageEngineReader, offset, maxNodeKey, liveKeys);
    }

    final Cache<NamesCacheKey, Names> namesCache = storageEngineReader.getBufferManager().getNamesCache();
    final NamesCacheKey namesCacheKey = new NamesCacheKey(storageEngineReader.getDatabaseId(),
        storageEngineReader.getResourceId(), storageEngineReader.getRevisionNumber(), offset);
    return namesCache.get(namesCacheKey,
        (_, _) -> Names.copy(Names.fromStorage(storageEngineReader, offset, maxNodeKey, liveKeys)));
  }

  /**
   * Set the deserialized live entry node-key set for a dictionary offset (called during NamePage
   * deserialization). Package-private: only the page deserializer populates this.
   *
   * @param offset the dictionary offset
   * @param bitmap the persisted live entry node-keys
   */
  void putLiveEntryNodeKeys(final int offset, final Roaring64Bitmap bitmap) {
    liveEntryNodeKeys.put(offset, bitmap);
  }

  /**
   * The set of live entry node-keys to serialize for a dictionary offset. For a dictionary loaded
   * (and possibly mutated) this transaction the authoritative set is re-derived from its
   * {@link Names} (O(live)); otherwise the set carried forward from deserialization is used. For
   * offset 0 the XML {@code attributes} and JSON {@code jsonObjectKeys} dictionaries are mutually
   * exclusive within a resource, so OR-ing both loaded sets yields the active one.
   *
   * @param offset the dictionary offset
   * @return the live entry node-keys (never null; empty when the dictionary has no live names)
   */
  public Roaring64Bitmap getLiveEntryNodeKeysToSerialize(final int offset) {
    final Roaring64Bitmap derived = switch (offset) {
      case JSON_OBJECT_KEY_REFERENCE_OFFSET -> orLiveKeys(jsonObjectKeys, attributes);
      case ELEMENTS_REFERENCE_OFFSET -> elements == null
          ? null
          : elements.liveEntryNodeKeys();
      case NAMESPACE_REFERENCE_OFFSET -> namespaces == null
          ? null
          : namespaces.liveEntryNodeKeys();
      case PROCESSING_INSTRUCTION_REFERENCE_OFFSET -> processingInstructions == null
          ? null
          : processingInstructions.liveEntryNodeKeys();
      default -> null;
    };
    if (derived != null) {
      return derived; // a loaded dictionary is authoritative for this revision, even if now empty
    }
    final Roaring64Bitmap carried = liveEntryNodeKeys.get(offset);
    return carried != null
        ? carried
        : new Roaring64Bitmap();
  }

  private static Roaring64Bitmap orLiveKeys(final Names a, final Names b) {
    if (a == null && b == null) {
      return null;
    }
    final Roaring64Bitmap bitmap = new Roaring64Bitmap();
    if (a != null) {
      bitmap.or(a.liveEntryNodeKeys());
    }
    if (b != null) {
      bitmap.or(b.liveEntryNodeKeys());
    }
    return bitmap;
  }

  /**
   * Get raw name belonging to name key.
   *
   * @param key name key identifying name
   * @return raw name of name key, or {@code null} if not present
   */
  public String getName(final int key, final NodeKind nodeKind, final StorageEngineReader storageEngineReader) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getName(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getName(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getName(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getName(key);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getName(key);
      }
      case ARRAY -> "__array__";
      case OBJECT -> "__object__";
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Get number of nodes with the given name key.
   *
   * @param key name key identifying name
   * @return number of nodes with the given name key
   */
  public int getCount(final int key, final NodeKind nodeKind, final StorageEngineReader storageEngineReader) {
    return switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        yield elements.getCount(key);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        yield namespaces.getCount(key);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        yield attributes.getCount(key);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        yield processingInstructions.getCount(key);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        yield jsonObjectKeys.getCount(key);
      }
      case ARRAY -> numberOfArrays;
      default -> throw new IllegalStateException("No other node types supported!");
    };
  }

  /**
   * Create name key given a name.
   *
   * @param name name to create key for
   * @param nodeKind kind of node
   * @return the created key
   */
  public int setName(final String name, final NodeKind nodeKind, final StorageEngineWriter storageEngineReader) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        return elements.setName(name, storageEngineReader);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        return namespaces.setName(name, storageEngineReader);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        return attributes.setName(name, storageEngineReader);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        return processingInstructions.setName(name, storageEngineReader);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        return jsonObjectKeys.setName(name, storageEngineReader);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Adds {@code delta} occurrences to an EXISTING interned name's count in one record touch — the
   * batched sibling of {@link #setName}'s per-occurrence increment. JSON object keys only for now
   * (the parallel bulk importer's use case); other kinds extend the switch when they need it.
   *
   * @param key the interned name key
   * @param delta additional occurrences; must be positive
   * @param nodeKind kind of node, selecting the dictionary
   * @param storageEngineWriter the writer for the count-record modification
   */
  public void addCount(final int key, final int delta, final NodeKind nodeKind,
      final StorageEngineWriter storageEngineWriter) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineWriter, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        jsonObjectKeys.addCount(key, delta, storageEngineWriter);
      }
      default -> throw new IllegalStateException("addCount currently supports JSON object keys only");
    }
  }

  /**
   * Resolve the key a name owns without storing it or counting an occurrence — see
   * {@code StorageEngineWriter#keyForName}.
   *
   * @param name name to resolve
   * @param nodeKind kind of node, selecting the dictionary
   * @param storageEngineReader the reader, used to materialize the dictionary on first touch
   * @return the key for the name
   */
  public int keyForName(final String name, final NodeKind nodeKind, final StorageEngineReader storageEngineReader) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        return elements.keyForName(name);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        return namespaces.keyForName(name);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        return attributes.keyForName(name);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        return processingInstructions.keyForName(name);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        return jsonObjectKeys.keyForName(name);
      }
      // Mirrors getName: these two kinds have no dictionary at all, their name is synthesized on
      // read ("__array__" / "__object__"), so the stored key is never resolved through a name
      // table. Keep handing back the bare hash the callers stored before.
      case ARRAY, OBJECT -> {
        return NamePageHash.generateHashForString(name);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Read the FSST symbol table with the given id.
   *
   * <p>
   * Fetched one record at a time rather than materialised as a whole dictionary the way names are.
   * Names are looked up by key thousands of times per page and are worth holding in a map; symbol
   * tables are looked up once per page and there are a handful per resource, so paying for the whole
   * trie to answer one question would be backwards.
   *
   * @param id the dictionary id, which is the record's node key
   * @param databaseType the database type, which fixes the dictionary offset
   * @param storageEngineReader the reader positioned at the revision whose table is wanted
   * @return the serialized symbol table, or {@code null} if no table with that id exists in this
   *         revision
   * @throws IllegalArgumentException if {@code id} is not a valid record key
   */
  public byte[] getFsstSymbolTable(final long id, final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader) {
    if (id <= 0) {
      throw new IllegalArgumentException("symbol table id must be positive, got " + id);
    }
    final var record = storageEngineReader.getRecord(id, IndexType.NAME, fsstSymbolTableOffset(databaseType));
    if (record == null) {
      return null;
    }
    if (!(record instanceof FsstSymbolTableNode symbolTable)) {
      throw new IllegalStateException(
          "record " + id + " in the FSST symbol-table tree is a " + record.getKind() + ", not a symbol table");
    }
    return symbolTable.getTable();
  }

  /**
   * Append a new FSST symbol table and return the id pages should refer to it by.
   *
   * <p>
   * Always appends. Overwriting an existing table would be the natural way to "update the
   * dictionary", and it would silently corrupt every page in every earlier revision that still points
   * at the old one — the compressed bytes on those pages are only meaningful against the exact table
   * they were encoded with. Copy-on-write keeps the old record reachable from the old revision root;
   * a new id keeps the new one from displacing it.
   *
   * @param table the serialized symbol table; must not be empty, since "no compression" is expressed
   *        by pages omitting the reference rather than by an empty table
   * @param databaseType the database type, needed to root the sub-trie on first use
   * @param storageEngineWriter the writer for the revision being built
   * @param log the transaction intent log of the revision being built
   * @return the id of the newly stored table
   * @throws IllegalArgumentException if {@code table} is empty
   */
  public long setFsstSymbolTable(final byte[] table, final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    Objects.requireNonNull(table, "table must not be null");
    Objects.requireNonNull(databaseType, "databaseType must not be null");
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    Objects.requireNonNull(log, "log must not be null");
    if (table.length == 0) {
      throw new IllegalArgumentException("refusing to store an empty symbol table; pages omit the reference instead");
    }
    final int offset = fsstSymbolTableOffset(databaseType);
    // Idempotent — it inspects the reference and returns early once the tree exists. Resources
    // that never enable FSST never reach here, so they never grow the delegate.
    createNameDictionaryTree(databaseType, storageEngineWriter, offset, log);
    // The id has to be chosen before the record is built, because a record is stored under the
    // node key it carries — and the record must be FILED under that same key. createRecord is
    // unusable here: it allocates a second key from this counter and picks the target record
    // page from THAT key, so the moment id and id+1 straddle a record-page boundary the node
    // lands on a page its own key does not address and becomes unreachable. persistRecord
    // derives the page from the record's own key. One key per table; the latest stored table is
    // always at exactly {@link #getLatestFsstSymbolTableId}. Ids are opaque to callers; all
    // that is promised is that they are positive, increasing, and never reused.
    final long id = incrementAndGetMaxNodeKey(offset);
    storageEngineWriter.persistRecord(new FsstSymbolTableNode(id, table), IndexType.NAME, offset);
    return id;
  }

  /**
   * Read one record of the global projection value dictionary.
   *
   * <p>
   * Fetched one record at a time rather than materialised as a whole dictionary the way names are,
   * and that is the point of the structure: a high-cardinality column holds millions of values, so
   * the {@link Names}-style "load the map, answer from memory" shape is exactly the behaviour that
   * has to be avoided (it is the name-dictionary blow-up in a new place). Records are buffer-managed
   * pages, so a repeated lookup costs a cache hit and a cold one costs the page holding the wanted id
   * and nothing else.
   *
   * @param nodeKey the record's node key, produced by the caller's namespace key layout
   * @param databaseType the database type, which fixes the dictionary offset
   * @param storageEngineReader the reader positioned at the revision whose dictionary is wanted
   * @return the record, or {@code null} if no record with that key exists in this revision
   * @throws IllegalArgumentException if {@code nodeKey} is not positive
   */
  public @Nullable DataRecord getProjectionValueDictionaryRecord(final long nodeKey, final DatabaseType databaseType,
      final StorageEngineReader storageEngineReader) {
    if (nodeKey <= 0) {
      throw new IllegalArgumentException("value dictionary node key must be positive, got " + nodeKey);
    }
    // Writer-scoped memo (no-op defaults for read-only transactions): once the async flush has
    // released a dictionary page from the intent log, an uncached read here decodes the WHOLE page
    // — IO, checksum, LZ77, version combine — for one record, and the radix walks re-read the same
    // upper-level nodes constantly. Soundness (CoW fresh keys, header evict-on-put, single thread)
    // is argued on the interface hooks.
    final DataRecord cached = storageEngineReader.cachedProjectionDictionaryRecord(nodeKey);
    if (cached != null) {
      return cached;
    }
    // Cross-TRANSACTION retention. The memo above belongs to one writer; a read view retains blocks
    // only for its own lifetime and is rebuilt per query execution, so without this every execution
    // re-decodes the dictionary material it touches -- measured as 26,300 LZ77 decode dispatches
    // over a 43-query leg with three global columns against 125 with none. Keyed by revision as
    // well as node key: the sub-trie is copy-on-write with fresh keys, and the one record rewritten
    // under a stable key (the generation header) is evicted by the put path below.
    final GlobalDictionaryRecordCacheKey recordCacheKey =
        new GlobalDictionaryRecordCacheKey(storageEngineReader.getDatabaseId(), storageEngineReader.getResourceId(),
            storageEngineReader.getRevisionNumber(), nodeKey);
    final Cache<GlobalDictionaryRecordCacheKey, DataRecord> recordCache =
        storageEngineReader.getBufferManager().getGlobalDictionaryRecordCache();
    // A WRITER never reads through this cache and never fills it. Its own memo above already serves
    // it, and a revision number does not identify content while a write transaction is open: an
    // aborted transaction's records would stay cached under a revision a later transaction goes on
    // to build, and key reuse would then serve content that was never committed. The writer memo is
    // bounded by one transaction and evicted by the put path, so it does not have that exposure.
    final boolean writing = storageEngineReader instanceof StorageEngineWriter;
    if (!writing) {
      final DataRecord retained = recordCache.get(recordCacheKey);
      if (retained != null) {
        return retained;
      }
    }
    final DataRecord record =
        storageEngineReader.getRecord(nodeKey, IndexType.NAME, projectionValueDictionaryOffset(databaseType));
    if (record != null) {
      storageEngineReader.cacheProjectionDictionaryRecord(nodeKey, record);
      if (!writing) {
        recordCache.put(recordCacheKey, record);
      }
    }
    return record;
  }

  /**
   * Store one record of the global projection value dictionary under the key it carries.
   *
   * <p>
   * Uses {@code persistRecord} rather than {@code createRecord} for the same reason
   * {@link #setFsstSymbolTable} does: the caller chooses the key (it encodes the namespace and the
   * id), and only {@code persistRecord} derives the target record page from the record's own key.
   * {@code createRecord} would allocate a different key from this page's counter and file the record
   * on a page its own key does not address.
   *
   * @param record the record to store, carrying its own node key
   * @param databaseType the database type, needed to root the sub-trie on first use
   * @param storageEngineWriter the writer for the revision being built
   * @param log the transaction intent log of the revision being built
   */
  public void putProjectionValueDictionaryRecord(final DataRecord record, final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    Objects.requireNonNull(record, "record must not be null");
    Objects.requireNonNull(databaseType, "databaseType must not be null");
    Objects.requireNonNull(storageEngineWriter, "storageEngineWriter must not be null");
    Objects.requireNonNull(log, "log must not be null");
    // Evict BEFORE persisting: nearly every dictionary write lands under a freshly minted key, but
    // the generation header is rewritten under its stable key, and a memoized pre-rewrite header
    // would resurrect a stale entry count on the next read.
    storageEngineWriter.evictProjectionDictionaryRecord(record.getNodeKey());
    // The cross-transaction cache needs the same eviction, and for the same record: the
    // generation header is the one key that is rewritten in place.
    storageEngineWriter.getBufferManager()
                       .getGlobalDictionaryRecordCache()
                       .remove(new GlobalDictionaryRecordCacheKey(storageEngineWriter.getDatabaseId(),
                           storageEngineWriter.getResourceId(), storageEngineWriter.getRevisionNumber(),
                           record.getNodeKey()));
    createProjectionValueDictionaryTree(databaseType, storageEngineWriter, log);
    storageEngineWriter.persistRecord(record, IndexType.NAME, projectionValueDictionaryOffset(databaseType));
  }

  /**
   * Root the value-dictionary sub-trie, and the offsets below it that would otherwise leave a gap.
   * Idempotent.
   *
   * <p>
   * The second half is what makes this more than a one-liner. This page's bookkeeping is serialized
   * positionally, so the offsets in use must be exactly {@code 0..n-1} — see
   * {@link #getDictionaryOffsetCount()}, which throws rather than write a gapped map. The FSST offset
   * immediately below this one is only registered when a resource actually stores a symbol table, so
   * a resource that never enabled FSST holds {0} and creating this tree alone would leave it holding
   * {0, 2}: not a wrong answer later, but a hard failure at the next commit. Rooting the symbol-table
   * tree first costs one empty page on a resource that will never use it, and is the only way the run
   * stays gapless without the caller having to know any of this.
   *
   * @param databaseType the database type, which fixes the offsets
   * @param storageEngineWriter the writer for the revision being built
   * @param log the transaction intent log of the revision being built
   */
  public void createProjectionValueDictionaryTree(final DatabaseType databaseType,
      final StorageEngineWriter storageEngineWriter, final TransactionIntentLog log) {
    createNameDictionaryTree(databaseType, storageEngineWriter, fsstSymbolTableOffset(databaseType), log);
    createNameDictionaryTree(databaseType, storageEngineWriter, projectionValueDictionaryOffset(databaseType), log);
  }

  /**
   * Whether the global projection value dictionary sub-trie exists in this revision. A reader uses it
   * to skip the probe entirely on a resource that has none.
   *
   * @param databaseType the database type, which fixes the dictionary offset
   * @return whether the sub-trie was ever rooted
   */
  public boolean hasProjectionValueDictionary(final DatabaseType databaseType) {
    final int offset = projectionValueDictionaryOffset(databaseType);
    final PageReference reference = referenceAtSlot(offset);
    return reference != null && (reference.getPage() != null || reference.getKey() != Constants.NULL_ID_LONG
        || reference.getLogKey() != Constants.NULL_ID_INT);
  }

  /**
   * The id of the most recently stored FSST symbol table, or {@code 0} when none was ever stored.
   * This is the single place that knows how ids relate to the dictionary's key counter — every reuse
   * and insert-time path resolves "the latest table" through it.
   *
   * @param databaseType the database type, which fixes the dictionary offset
   * @return the latest table id, or {@code 0} for none
   */
  public long getLatestFsstSymbolTableId(final DatabaseType databaseType) {
    return getMaxNodeKey(fsstSymbolTableOffset(databaseType));
  }

  /**
   * The dictionary offsets this page currently holds bookkeeping for, as a gapless count.
   *
   * <p>
   * The keyed-trie node counters, level counters and live-key bitmaps are written positionally — a
   * count, then one entry per offset from 0 upwards — so the dictionary offsets in use must be
   * exactly {@code 0..n-1}. The secondary HOT page-key map is deliberately excluded and serialized
   * as explicit sparse pairs. Dictionary offsets have always been allocated as a contiguous block
   * per database type, but nothing enforced it, and a gap does not fail: the positional writer emits
   * the wrong count, fabricates a zero for the missing offset, and drops the highest one. The result
   * is a resource that reloads with a dictionary silently truncated to nothing.
   *
   * @return the number of offsets, i.e. one past the highest offset in use
   * @throws IllegalStateException if the offsets in use have a gap
   */
  public int getDictionaryOffsetCount() {
    if (maxNodeKeys.isEmpty()) {
      return 0;
    }
    int highest = -1;
    for (final int offset : maxNodeKeys.keySet()) {
      if (offset > highest) {
        highest = offset;
      }
    }
    final int count = highest + 1;
    if (maxNodeKeys.size() != count) {
      throw new IllegalStateException("NamePage dictionary offsets must be gapless to serialize; "
          + "highest offset is " + highest + " but only " + maxNodeKeys.size()
          + " offsets are present — a positional write would drop the highest and fabricate the "
          + "missing one. Offsets in use: " + new TreeSet<>(maxNodeKeys.keySet()));
    }
    return count;
  }

  /**
   * Get the size of CurrentMaxLevelOfIndirectPage to Serialize
   *
   * @return int Size of CurrentMaxLevelOfIndirectPage
   */
  public int getCurrentMaxLevelOfIndirectPagesSize() {
    return currentMaxLevelsOfIndirectPages.size();
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
  }

  @Override
  public String toString() {
    return ToStringHelper.of(this)
                         .add("elements", elements)
                         .add("attributes", attributes)
                         .add("URIs", namespaces)
                         .add("PIs", processingInstructions)
                         .toString();
  }

  /**
   * Remove an attribute-name.
   *
   * @param key the key to remove
   */
  public void removeName(final int key, final NodeKind nodeKind, final StorageEngineWriter storageEngineReader) {
    // $CASES-OMITTED$
    switch (nodeKind) {
      case ELEMENT -> {
        if (elements == null) {
          elements = getNames(storageEngineReader, ELEMENTS_REFERENCE_OFFSET);
        }
        elements.removeName(key, storageEngineReader);
      }
      case NAMESPACE -> {
        if (namespaces == null) {
          namespaces = getNames(storageEngineReader, NAMESPACE_REFERENCE_OFFSET);
        }
        namespaces.removeName(key, storageEngineReader);
      }
      case ATTRIBUTE -> {
        if (attributes == null) {
          attributes = getNames(storageEngineReader, ATTRIBUTES_REFERENCE_OFFSET);
        }
        attributes.removeName(key, storageEngineReader);
      }
      case PROCESSING_INSTRUCTION -> {
        if (processingInstructions == null) {
          processingInstructions = getNames(storageEngineReader, PROCESSING_INSTRUCTION_REFERENCE_OFFSET);
        }
        processingInstructions.removeName(key, storageEngineReader);
      }
      case OBJECT_NAMED_OBJECT, OBJECT_NAMED_ARRAY, OBJECT_NAMED_BOOLEAN, OBJECT_NAMED_NUMBER, OBJECT_NAMED_STRING,
          OBJECT_NAMED_NULL -> {
        if (jsonObjectKeys == null) {
          jsonObjectKeys = getNames(storageEngineReader, JSON_OBJECT_KEY_REFERENCE_OFFSET);
        }
        jsonObjectKeys.removeName(key, storageEngineReader);
      }
      default -> throw new IllegalStateException("No other node types supported!");
    }
  }

  /**
   * Initialize a keyed-trie name dictionary.
   *
   * @param databaseType The type of database.
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createNameDictionaryTree(final DatabaseType databaseType, final StorageEngineReader storageEngineReader,
      final int index, final TransactionIntentLog log) {
    requireNameDictionarySlot(databaseType, index);
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createKeyedTrie(databaseType, reference, IndexType.NAME, storageEngineReader, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.put(index, 0);
    }
  }

  /**
   * Initialize the HOT (Height Optimized Trie) secondary NAME index tree.
   *
   * <p>
   * Creates the canonical secondary NAME index root. HOT page keys are allocated independently
   * through {@link #incrementAndGetMaxHotPageKey(int)}. In particular, a secondary index must never
   * enter {@link #maxNodeKeys} or {@link #currentMaxLevelsOfIndirectPages}: those maps describe the
   * keyed tries used by name dictionaries, FSST and the projection-value dictionary.
   * </p>
   *
   * @param storageEngineReader {@link StorageEngineReader} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createNameIndexTree(final StorageEngineReader storageEngineReader, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createHOTTree(reference, IndexType.NAME, storageEngineReader, log);
    }
  }

  /**
   * Return an existing keyed-trie dictionary root without creating a structural placeholder.
   *
   * @param databaseType database type defining the reserved dictionary prefix
   * @param index physical reserved dictionary slot
   * @return the existing dictionary root, or {@code null} if it has not been initialized
   * @throws IllegalArgumentException if {@code index} belongs to a secondary NAME index
   */
  public @Nullable PageReference getNameDictionaryReference(final DatabaseType databaseType, final int index) {
    requireNameDictionarySlot(databaseType, index);
    return referenceAtSlot(index);
  }

  private static void requireNameDictionarySlot(final DatabaseType databaseType, final int index) {
    if (!isNameDictionarySlot(databaseType, index)) {
      throw new IllegalArgumentException("NamePage slot " + index + " is not a reserved " + databaseType
          + " dictionary slot; secondary NAME indexes use HOT storage");
    }
  }

  /**
   * Return the first secondary NAME index slot whose physical HOT tree has never been initialized.
   *
   * <p>Dropping an index removes its current catalog definition, but its copy-on-write tree remains
   * reachable for historical revisions and therefore must never be assigned to a different index.
   * A persisted HOT page-key high-water mark or a non-virgin root reference reserves the slot. A
   * read-side placeholder does not. The scan starts after the database type's dictionary, FSST and
   * projection-value slots, and never creates a reference while probing.</p>
   *
   * @param databaseType database type defining the reserved NamePage prefix
   * @return the first physical slot that has never held a secondary NAME index
   * @throws IllegalStateException if every secondary NAME slot has been initialized
   */
  public int nextUnallocatedSecondaryNameIndex(final DatabaseType databaseType) {
    final int firstSecondaryIndex = firstSecondaryNameIndex(databaseType);
    for (int index = firstSecondaryIndex; index < Constants.INP_REFERENCE_COUNT; index++) {
      if (!secondaryNameIndexInitialized(index)) {
        return index;
      }
    }
    throw new IllegalStateException("Secondary NAME index reference space exhausted for " + databaseType
        + ": all slots from " + firstSecondaryIndex + " through " + (Constants.INP_REFERENCE_COUNT - 1)
        + " have been initialized");
  }

  /**
   * Determine whether a physical secondary NAME slot has ever owned a HOT tree.
   *
   * <p>This is a non-mutating probe: it neither grows the reference delegate nor materializes a
   * placeholder. A persisted HOT page-key high-water mark and a non-virgin root reference are the
   * two durable witnesses, matching {@link #nextUnallocatedSecondaryNameIndex(DatabaseType)}.</p>
   *
   * @param databaseType database type defining the first secondary slot
   * @param index physical secondary NAME slot
   * @return {@code true} if the slot has allocator metadata or a non-virgin root
   * @throws IllegalArgumentException if {@code index} is outside the secondary NAME slot range
   */
  public boolean isSecondaryNameIndexInitialized(final DatabaseType databaseType, final int index) {
    validateSecondaryNameIndexSlot(databaseType, index);
    return secondaryNameIndexInitialized(index);
  }

  private boolean secondaryNameIndexInitialized(final int index) {
    if (maxHotPageKeys.containsKey(index)) {
      return true;
    }
    final PageReference reference = referenceAtSlot(index);
    return reference != null && !reference.isVirginStructuralPlaceholder();
  }

  /**
   * Return an existing secondary NAME HOT root without creating a structural placeholder.
   *
   * @param databaseType database type defining the secondary slot range
   * @param index physical secondary NAME slot
   * @return the existing root reference, or {@code null} if the slot has none
   * @throws IllegalArgumentException if {@code index} is a reserved dictionary slot or outside the
   *         page's reference space
   */
  public @Nullable PageReference getIndexReference(final DatabaseType databaseType, final int index) {
    validateSecondaryNameIndexSlot(databaseType, index);
    return referenceAtSlot(index);
  }

  private static void validateSecondaryNameIndexSlot(final DatabaseType databaseType, final int index) {
    final int firstSecondaryIndex = firstSecondaryNameIndex(databaseType);
    if (index < firstSecondaryIndex || index >= Constants.INP_REFERENCE_COUNT) {
      throw new IllegalArgumentException("NamePage slot " + index + " is outside the secondary " + databaseType
          + " NAME range [" + firstSecondaryIndex + ", " + Constants.INP_REFERENCE_COUNT + ")");
    }
  }

  private @Nullable PageReference referenceAtSlot(final int index) {
    return switch (delegate) {
      case ReferencesPage4 references -> references.referenceAtOffset(index);
      case BitmapReferencesPage references -> references.referenceAtOffset(index);
      case FullReferencesPage references -> references.referenceAt(index);
      default -> throw new IllegalStateException(
          "Unknown NamePage delegate type: " + delegate.getClass().getName());
    };
  }

  public int getNumberOfArrays() {
    return numberOfArrays;
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNumber the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNumber) {
    return maxNodeKeys.get(indexNumber);
  }

  /**
   * Get the size of MaxNodeKey to Serialize
   * 
   * @return int Size of MaxNodeKey
   */
  public int getMaxNodeKeySize() {
    return maxNodeKeys.size();
  }

  public long incrementAndGetMaxNodeKey(final int indexNumber) {
    final long newMaxNodeKey = maxNodeKeys.getOrDefault(indexNumber, 0L) + 1;
    maxNodeKeys.put(indexNumber, newMaxNodeKey);
    return newMaxNodeKey;
  }

  /**
   * Reserve a contiguous run of node keys in the global projection value dictionary's sub-trie and
   * return the first of them.
   *
   * <p>
   * <b>Why a run, and why it has to be contiguous and monotonic.</b> The indirect-page trie a
   * sub-trie is built from grows a level only when the page key being prepared is exactly the
   * power-of-two boundary of the current height ({@code KeyedTrieWriter#prepareLeafOfTree}). That
   * makes dense, monotonically allocated keys not a convention but a requirement: a key space with a
   * stride jumps past every boundary without ever triggering growth, and the traversal then resolves
   * every page key to the root reference — so records at wildly different keys silently land on one
   * page and overwrite each other. Allocating from this counter is the only way to get keys the trie
   * can address, which is why the dictionary records where its run starts instead of computing its
   * keys from a namespace.
   *
   * <p>
   * Never reset. A rebuild reserves a fresh run rather than reusing the old one, which leaves the
   * previous run's records in the trie unreferenced by any live dictionary — the same append-only,
   * never-reclaimed treatment {@link #setFsstSymbolTable} gives symbol tables, and for the same
   * reason: an earlier revision may still be reading them.
   *
   * @param count how many keys to reserve
   * @return the first key of the run
   * @throws IllegalArgumentException if {@code count} is not positive
   */
  public long reserveProjectionValueDictionaryKeys(final DatabaseType databaseType, final long count) {
    if (count <= 0) {
      throw new IllegalArgumentException("must reserve a positive number of keys, got " + count);
    }
    final int indexNumber = projectionValueDictionaryOffset(databaseType);
    final long first = Math.addExact(maxNodeKeys.getOrDefault(indexNumber, 0L), 1L);
    final long last = Math.addExact(first, Math.subtractExact(count, 1L));
    maxNodeKeys.put(indexNumber, last);
    return first;
  }

  /**
   * Get the maximum HOT page key of the specified index by its index number.
   *
   * @param indexNo the index number
   * @return the maximum HOT page key stored
   */
  public long getMaxHotPageKey(final int indexNo) {
    return maxHotPageKeys.get(indexNo);
  }

  /**
   * Get the size of maxHotPageKeys for serialization.
   *
   * @return number of entries
   */
  public int getMaxHotPageKeySize() {
    return maxHotPageKeys.size();
  }

  Int2LongMap maxHotPageKeysForSerialization() {
    return maxHotPageKeys;
  }

  /**
   * Increment and get the maximum HOT page key for the given index.
   *
   * @param indexNo the index number
   * @return the new maximum HOT page key
   */
  public long incrementAndGetMaxHotPageKey(final int indexNo) {
    final long newKey = maxHotPageKeys.get(indexNo) + 1;
    maxHotPageKeys.put(indexNo, newKey);
    return newKey;
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }
}
