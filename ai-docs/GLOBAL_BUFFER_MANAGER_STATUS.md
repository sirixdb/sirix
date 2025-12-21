# Global BufferManager Refactoring - Implementation Status

## Overview
Refactoring SirixDB to use a single global BufferManager across all databases and resources, following PostgreSQL/MySQL/SQL Server architecture patterns.

## ✅ Completed Work

### Phase 1: Database ID Infrastructure (COMPLETE)
- ✅ Added `databaseId` field to `DatabaseConfiguration` 
- ✅ Added `getDatabaseId()` and `setDatabaseId(long)` methods
- ✅ Updated serialization to write `databaseId` to JSON
- ✅ Updated deserialization to read `databaseId` from JSON with backward compatibility
- ✅ Added `DATABASE_ID_COUNTER` in `Databases.java`
- ✅ Database ID auto-assignment in `createTheDatabase()`
- ✅ Database ID auto-assignment when opening existing databases (backward compatibility)

**Files Modified:**
- `bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/Databases.java`

### Phase 4: PageReference Composite Keys (COMPLETE)
- ✅ Added `databaseId` field to `PageReference`
- ✅ Added `resourceId` field to `PageReference`
- ✅ Added getters and setters for both IDs (with hash reset)
- ✅ Updated `hashCode()` to include both IDs: `Objects.hash(databaseId, resourceId, logKey, key)`
- ✅ Updated `equals()` to check both IDs
- ✅ Updated `toString()` to display both IDs
- ✅ Updated copy constructor to copy both IDs

**Files Modified:**
- `bundles/sirix-core/src/main/java/io/sirix/page/PageReference.java`

### Phase 6: Delegate Page Classes (COMPLETE)
- ✅ Updated `BitmapReferencesPage` to copy both IDs when cloning
- ✅ Updated `ReferencesPage4` to copy both IDs when cloning
- ✅ Updated `FullReferencesPage` to copy both IDs when cloning

**Files Modified:**
- `bundles/sirix-core/src/main/java/io/sirix/page/delegates/BitmapReferencesPage.java`
- `bundles/sirix-core/src/main/java/io/sirix/page/delegates/ReferencesPage4.java`
- `bundles/sirix-core/src/main/java/io/sirix/page/delegates/FullReferencesPage.java`

### Phase 8: Resource ID Access (COMPLETE)
- ✅ Added `getDatabaseId()` method to `ResourceConfiguration`
- ✅ Method returns database ID from parent `DatabaseConfiguration`

**Files Modified:**
- `bundles/sirix-core/src/main/java/io/sirix/access/ResourceConfiguration.java`

### Phase 2: Update All Cache Key Structures (COMPLETE)
- ✅ Created `RevisionRootPageCacheKey` record with (databaseId, resourceId, revision)
- ✅ Created `PathSummaryCacheKey` record with (databaseId, resourceId, pathNodeKey)
- ✅ Updated `NamesCacheKey` to include databaseId and resourceId
- ✅ Updated `RBIndexKey` to include databaseId and resourceId
- ✅ Updated `BufferManager` interface with new cache type signatures
- ✅ Updated `BufferManagerImpl` to match new interface
- ✅ Updated `RevisionRootPageCache` to use `RevisionRootPageCacheKey`
- ✅ Updated `PathSummaryCache` to use `PathSummaryCacheKey`
- ✅ Updated `EmptyBufferManager` with new cache types

**Files Created:**
- `bundles/sirix-core/src/main/java/io/sirix/cache/RevisionRootPageCacheKey.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/PathSummaryCacheKey.java`

**Files Modified:**
- `bundles/sirix-core/src/main/java/io/sirix/cache/NamesCacheKey.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/RBIndexKey.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/BufferManager.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/BufferManagerImpl.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/RevisionRootPageCache.java`
- `bundles/sirix-core/src/main/java/io/sirix/cache/PathSummaryCache.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/EmptyBufferManager.java`

## 🚧 Remaining Work

### Phase 2: Update All Cache Key Structures (COMPLETE - SEE ABOVE)

This is extensive work requiring creation of new files and updates to existing ones:

#### New Files to Create:
1. **RevisionRootPageCacheKey.java**
   ```java
   public record RevisionRootPageCacheKey(long databaseId, long resourceId, int revision) {}
   ```

2. **PathSummaryCacheKey.java**
   ```java
   public record PathSummaryCacheKey(long databaseId, long resourceId, int pathNodeKey) {}
   ```

#### Existing Files to Update:
3. **NamesCacheKey.java** - Add `databaseId` and `resourceId` parameters
4. **RBIndexKey.java** - Add `databaseId` and `resourceId` parameters
5. **BufferManager.java** - Update method signatures for new cache key types
6. **BufferManagerImpl.java** - Update cache declarations
7. **EmptyBufferManager.java** - Update empty cache types
8. **RevisionRootPageCache.java** - Change key type from `Integer` to `RevisionRootPageCacheKey`
9. **PathSummaryCache.java** - Change key type from `Integer` to `PathSummaryCacheKey`

### Phase 3: Single Global BufferManager (NOT STARTED)

**Files to Update:**
1. **Databases.java**
   - Replace `BUFFER_MANAGERS` map with single `GLOBAL_BUFFER_MANAGER`
   - Update `getBufferManager()` to return single instance
   - Update `removeDatabase()` and `closeDatabase()`

2. **LocalDatabase.java**
   - Change `bufferManagers` field from Map to single `BufferManager` reference
   - Remove `addResourceToBufferManagerMapping()` method
   - Update constructor and all resource session creation

### Phase 5: Propagate IDs Through Transaction System (NOT STARTED)

**Critical Files:**
1. **NodePageReadOnlyTrx.java**
   - Store `databaseId` and `resourceId` from `ResourceConfiguration`
   - Set IDs on all `PageReference` creations
   - Update ALL cache access points to use new composite keys:
     - `RevisionRootPageCache` access → use `RevisionRootPageCacheKey`
     - `NamesCache` access → use updated `NamesCacheKey`
     - `PathSummaryCache` access → use `PathSummaryCacheKey`
     - `RBIndexKey` creation → include both IDs

2. **NodePageTrx.java**
   - Similar changes as `NodePageReadOnlyTrx`
   - Set IDs on all new PageReference instances
   - Use composite keys for all cache operations

### Phase 6: Update Delegate Page Classes (NOT STARTED)

**Files:**
- `ReferencesPage4.java` - Copy IDs when cloning PageReferences
- `BitmapReferencesPage.java` - Copy IDs when cloning PageReferences  
- `FullReferencesPage.java` - Copy IDs when cloning PageReferences

### Phase 7: Update All PageReference Creation Sites (NOT STARTED)

**Need to find all `new PageReference()` calls and ensure IDs are set:**
- `IndirectPage.java`
- `PathPage.java`
- `CASPage.java`
- `NamePage.java`
- `UberPage.java`
- All page reader/writer classes
- Any other page classes

### Phase 9: Testing (NOT STARTED)

**Unit Tests Needed:**
- Test PageReference equality with different database/resource IDs
- Test each cache key structure independently
- Verify hash codes are unique for different (databaseId, resourceId) combinations

**Integration Tests Needed:**
- Test with multiple databases open simultaneously
- Verify no cache collisions between databases/resources
- Test concurrent access across databases
- Verify cache eviction works correctly
- Test buffer pool memory limits with global sharing

**Backward Compatibility Tests:**
- Test loading existing databases without database ID
- Verify automatic ID assignment
- Test migration path

## Estimated Scope

**Files Modified So Far:** 16
**New Files Created:** 2
**Files Remaining:** 15+ (estimated)

**Critical Path:**
1. Complete Phase 2 (cache keys) - Foundation for all caches
2. Complete Phase 3 (global BufferManager) - Core architectural change
3. Complete Phase 5 (transaction system) - Wire everything together
4. Complete Phases 6-7 (all creation sites) - Ensure consistency
5. Complete Phase 9 (testing) - Validation

## Next Steps

The implementation should proceed in this order:

1. **Create new cache key classes** (Phase 2.1-2.4)
2. **Update existing cache key classes** (Phase 2.2-2.3)
3. **Update BufferManager interfaces** (Phase 2.5-2.6)
4. **Convert to global BufferManager** (Phase 3)
5. **Update transaction system** (Phase 5)
6. **Update all delegate and page classes** (Phases 6-7)
7. **Testing and validation** (Phase 9)

## Benefits Upon Completion

1. **Correctness**: Prevents ALL cache collisions across databases/resources
2. **Industry Standard**: Matches PostgreSQL/MySQL/SQL Server architecture
3. **Scalability**: Single buffer pool enables better global memory management
4. **Performance**: Reduces memory overhead of multiple BufferManager instances
5. **Simplicity**: Eliminates complex nested BufferManager maps

## Risks and Considerations

- **Thread Safety**: Global BufferManager must handle concurrent access from all databases
- **Memory Limits**: Single buffer pool shares memory across all databases
- **Fair Eviction**: Cache eviction must not starve any database/resource
- **Testing**: Extensive testing required to ensure no regressions
- **Migration**: Existing deployments need smooth upgrade path

