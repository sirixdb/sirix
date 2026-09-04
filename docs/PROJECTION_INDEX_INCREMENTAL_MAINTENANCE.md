# Projection Index Record Lookup and Incremental Maintenance

Status: normative design and implementation contract for the V0 projection
storage format.

This document explains how a source record is found inside a projection index,
then follows updates, deletes, inserts, and moves through the index. It also
states the locality, versioning, corruption-detection, and allocation contracts
that the implementation must preserve.

The storage format described here is intentionally V0-only. There is no legacy
projection database to migrate, so the V0 bytes may be defined correctly rather
than carrying a compatibility branch for an unsuitable earlier layout.

## 1. The central distinction: identity is not order

Every persistent Sirix document node has a stable `nodeKey`. A projection uses
the `nodeKey` of a projected record's root as its `recordKey`.

A `recordKey` answers:

> Which source record is this projection row derived from?

It does not answer:

> Where does this record occur in document order?

Node keys are allocation identities. An insertion receives a new key; existing
siblings are not renumbered. For example, the records with keys `2`, `5`, and
`8` can receive a newly allocated record `11` between `2` and `5`:

```text
before: 2, 5, 8
after:  2, 11, 5, 8
```

Sorting those keys would produce `2, 5, 8, 11`, which is not the document.
Consequently, the projection index must store identity and order separately:

- `KEYS` stores each row's source `recordKey` explicitly.
- Rows inside a row group are stored in document order.
- Row groups have explicit previous/next links in document order.
- Numeric fences route only the increasing subset of record keys called the
  normal backbone.
- A sparse exact locator routes records that cannot participate in that
  backbone.

No update algorithm may recover document order by sorting node keys.

## 2. Complete running example

Consider this JSON document at revision 1:

```json
[
  {"id": 1, "price": 10},
  {"id": 2, "price": 20},
  {"id": 3, "price": 30}
]
```

The projection definition is:

```text
record root: /[]
column 0:    /[]/id     as numeric long
column 1:    /[]/price  as numeric long
```

These are the actual Sirix projection paths, not JSONPath shorthand. `/[]`
selects each top-level array item as a record root, and each column path is
absolute from the document root.

The following node keys are illustrative, but they show the important
relationships:

```text
node 0  document root
node 1  top-level array

node 2  first record object
node 3    fused id value 1       parent 2
node 4    fused price value 10   parent 2

node 5  second record object
node 6    fused id value 2       parent 5
node 7    fused price value 20   parent 5

node 8  third record object
node 9    fused id value 3       parent 8
node 10   fused price value 30   parent 8
```

Current storage represents those numeric properties as fused
`OBJECT_NAMED_NUMBER` nodes. Each fused property node carries both the field
name and value and is a direct child of its record object. The exact numeric
keys above are illustrative; the parent/record-root relationships are the
important part.

For simplicity, assume all three projection rows fit in physical row-group
slot 1. Its logical content is:

```text
physical row-group slot: 1
row:                     0   1   2
KEYS:                    2   5   8
id:                      1   2   3
price:                  10  20  30
order exception:         0   0   0
```

The row number is local to this row group. It is not stored in the document and
is not stable across row insertions or deletions. `KEYS[1] == 5` is what binds
local row 1 to source record 5.

## 3. How record 5 is found in the projection index

The lookup input is the stable source record key `5`. The projection performs
two routing stages and one exact verification.

### 3.1 Stage 1: try the sparse exception locator

The exception-locator namespace uses a negative HOT key:

```text
locatorSlotKey(recordKey) = Long.MIN_VALUE | recordKey
```

All ordinary projection storage slots are non-negative, so the two namespaces
cannot collide.

The lookup probes `locatorSlotKey(5)`. In the initial example no locator exists,
because key 5 is part of the normal increasing backbone `2, 5, 8`.

An absent locator does not yet mean that the record is absent. It means only
that normal fence routing must be tried.

### 3.2 Stage 2: route through the normal fences

The normal-key fence for physical slot 1 is:

```text
first normal key: 2
last normal key:  8
```

Because `2 <= 5 <= 8`, the fence structure selects physical slot 1 as the
candidate. At scale, the implementation first chooses an immutable base range
from monotone base high-water marks and then follows a bounded normal-only skip
structure within that base chain. It does not scan every row group.

The fence is only a routing summary. It does not prove that key 5 exists, and
it does not calculate a row number.

### 3.3 Translate physical slot 1 to persisted HOT slots

The fence result `physical row-group slot 1` is a logical row-group address,
not a HOT leaf-page number and not the local row ordinal. Projection segment
storage encodes it into composite HOT keys:

```text
descriptor HOT key = physicalSlot << 16
                   = 1 << 16
                   = 65536

KEYS segment id    = 0
KEYS HOT key       = (physicalSlot << 16) | (segmentId + 1)
                   = 65536 | 1
                   = 65537
```

`ProjectionPersistedRecordLookup.keys(1)` first reads and verifies descriptor
slot `65536`. It then uses descriptor entry 0 to read KEYS slot `65537` and
verifies the segment kind, length, and content hash. This descriptor/segment
indirection is why “row-group slot 1” must not be confused with “HOT key 1.”

### 3.4 Exact verification in `KEYS`

The projection reads the candidate row group's `KEYS` segment:

```text
KEYS: 2, 5, 8
```

It searches at most 1024 entries for the exact value `5`. The match is at local
row 1. All column vectors use the same local row ordinal, so:

```text
id[1]    == 2
price[1] == 20
```

This is the complete answer to how record 5 is found in the projection index:

1. No sparse locator exists for normal key 5.
2. The normal fence routes key 5 to physical slot 1.
3. Composite HOT keys `65536` and `65537` load the verified descriptor and
   `KEYS` segment.
4. `KEYS[1] == 5` proves the exact row.

There is deliberately no formula such as `row = recordKey - firstKey`.
Node-key gaps, deleted nodes, and inserted nodes make such a formula invalid.

### 3.5 Required normal-path validation

The normal path succeeds only when all of these facts agree:

- The exception locator is absent.
- Exactly one matching `KEYS` entry exists in the candidate row group.
- The matching row's order-exception bit is clear.
- The row group's normal fence covers the key.
- The physical slot is live in this revision.

If the `KEYS` entry is marked exceptional but its locator was absent, the index
is inconsistent. The implementation must fail closed; it must not silently
accept the row through the normal path.

## 4. How a changed document node resolves to record 5

Projection-row lookup begins only after the change listener has identified the
record root. These are separate operations.

Suppose node 7 changes from `20` to `25`. The current document cursor already
knows node 7's stable key and parent link. The listener walks the parent chain:

```text
price node 7 -> record object 5 -> projected array 1
```

The projection definition says that children of array 1 are record roots, so
the listener records dirty `recordKey == 5` and dirty column `price`.

This parent walk occurs in the document index. It is not a scan of JSON text,
and it is not a projection-index lookup. Sirix document navigation by
`nodeKey`, including `rtx.moveTo(5)`, is a keyed document-page lookup.

After resolving the root key, the projection performs the lookup from section
3 to find record 5's projection row.

## 5. Value update: price 20 becomes 25

Revision 2 changes only the second record's price:

```json
[
  {"id": 1, "price": 10},
  {"id": 2, "price": 25},
  {"id": 3, "price": 30}
]
```

The maintenance sequence is:

1. The listener resolves changed node 7 to record root 5.
2. It remembers that only the `price` projection column is dirty.
3. The sparse locator probe for 5 is absent.
4. Normal fences select physical slot 1.
5. Exact `KEYS` lookup finds 5 at local row 1 and proves which row group is
   affected.
6. The extractor re-reads the selected `price` column for every live record in
   that one row group, at most 1024 rows.
7. The writer re-encodes only that row group's price column segment and the
   small descriptor needed to reference and describe it.
8. It refreshes slot-0 metadata with the committing revision. Depending on the
   column shape, it may also patch that column's bounded Bloom, set-summary, or
   global-dictionary units.
9. `KEYS`, `id`, document-order links, normal fences, and exception metadata
   remain unchanged.

The resulting logical row group is:

```text
row:             0   1   2
KEYS:            2   5   8
id:              1   2   3
price:          10  25  30
exception:       0   0   0
```

The value-only path must not materialize, copy, or rewrite an order-exception
bitmap. It only needs the already encoded `KEYS` metadata for validation.

Re-extracting the whole selected column is intentional in V0. Zone maps and
aggregate provenance flags describe the complete segment, and V0 has no
per-row witness saying which cell established a sticky flag. Rebuilding one
bounded column segment can therefore remove stale min/max or provenance while
leaving every other column and `KEYS` untouched. The smallest persistent write
unit is a semantic column segment, not an individual cell.

Revision 1 continues to expose the old price value 20. Revision 2 exposes 25.
The projection writer does not rewrite unchanged semantic segment slots;
whether their containing physical pages are reconstructed or shared is decided
by the configured `VersioningType` and by whether a value is inline.

## 6. Delete: remove record 5

Revision 3 removes the second object:

```json
[
  {"id": 1, "price": 10},
  {"id": 3, "price": 30}
]
```

### 6.1 Why deletion provenance must be captured before removal

After physical deletion, `rtx.moveTo(5)` can no longer read the current source
record. Therefore the change notification must identify and retain record key
5 while its parent relationship is still readable.

The listener retains primitive deletion provenance, including the affected
record key and the structural context needed for membership/order maintenance.
It does not retain a detached JSON object or copy the deleted subtree.

### 6.2 Finding the old projection row

The current write transaction's projection still contains revision 2's row
until maintenance is applied. Record 5 is found exactly as before:

1. The exception locator is absent.
2. Its normal fence selects physical slot 1.
3. `KEYS[1] == 5` identifies local row 1.

The source record is no longer required for deletion. The persisted projection
row is sufficient to identify what must be removed.

### 6.3 Local rewrite

Removing local row 1 compacts only this row group:

```text
row:             0   1
KEYS:            2   8
id:              1   3
price:          10  30
exception:       0   0
```

Because row ordinals shifted, the row group's `KEYS` and every projected column
segment in that row group are semantically rebuilt. Content-addressed write
suppression may carry an accidentally byte-identical segment forward, but the
membership path must treat all columns as affected. Other row groups are not
rebuilt. The writer compares the old and new normal endpoints and updates the
local fence only if the first or last normal key changed. The delete above
keeps `[2, 8]`, so its fence chunk is not dirtied.

An ordinary delete does not trigger global compaction. If a locally allocated
split row group becomes empty, it may be unlinked and recycled. An immutable
base/sentinel row group remains as a routing anchor even when it becomes empty;
this avoids renumbering or rewriting every later physical slot.

## 7. Middle insert: why an exception locator is needed

Start again from revision 1 and insert this newly allocated record between the
first and second records:

```json
{"id": 4, "price": 15}
```

Assume its record root receives node key 11. Document order is now:

```text
2, 11, 5, 8
```

Key 11 cannot join the increasing normal backbone at that position. The local
row group becomes:

```text
row:             0   1   2   3
KEYS:            2  11   5   8
id:              1   4   2   3
price:          10  15  20  30
exception:       0   1   0   0
```

The normal backbone remains `2, 5, 8`, and the normal fence remains `[2, 8]`.
The writer persists one sparse locator:

```text
locator(recordKey 11) -> physical slot 1
```

Looking up key 11 now does this:

1. Probe its exact negative locator key.
2. Decode physical slot 1 from the locator value.
3. Verify that slot 1 is live.
4. Search slot 1's `KEYS` and find key 11 at local row 1.
5. Verify that local row 1's exception bit is set.

Both the locator and the bit are required. The locator provides fast routing;
the `KEYS` value and exception bit prove that it did not become stale or point
at unrelated bytes.

Normal key 5 is still found through the normal fence and exact `KEYS` search.
The fact that exception key 11 lies physically before it does not interfere
with normal routing.

## 8. Common-case exception encoding

Most initial ingestion, especially ordered LDJSON or a root JSON array, creates
records in document-tail order. Their node keys also increase, so almost every
row is normal.

The V0 `KEYS` segment therefore has two exception modes:

```text
marker 0: this row group has no order exceptions
marker 1: dense live exception words follow
```

For marker 0:

- No exception bitmap object is allocated.
- No bitmap words are persisted.
- An exception test returns false directly from the marker.
- A value-only column update preserves the encoded marker without decoding a
  bitmap.

For marker 1, only `ceil(rowCount / 64)` live words are stored. At the maximum
1024 rows this is 16 longs, or 128 bytes.

Membership or order changes expand exception flags for only the touched row
group into a primitive `BooleanArrayList`. Its backing `boolean[]` is bounded by
the 1024-row group (about 1 KiB of payload, plus headers), with no per-row boxed
objects. While rows are copied, each destination page allocates its dense
exception bitmap lazily on the first `true` flag: at most 16 longs for a
1024-row destination. The path does not repeatedly clone a 1024-bit bitmap.

The sparse locator is also absent for normal rows. Thus the normal ingestion
case pays neither per-record locator storage nor per-leaf bitmap allocation.

## 9. Tail insert

A newly created record may join the normal backbone only when the listener can
prove both of these facts:

- It is appended at the end of the projected record sequence.
- Its key is greater than the normal global high-water key.

For example, appending key 11 after `2, 5, 8` yields:

```text
KEYS:       2, 5, 8, 11
exception:  no-exceptions marker
normal:     2, 5, 8, 11
```

No sparse locator is written. Among the routing units, only the last normal
boundary is extended. The target `KEYS` and column segments, its descriptor,
slot-0 metadata, and any affected derived column metadata still change.

If either fact is not proven, the new row is classified as an exception. The
writer must not guess that an absent projection row is a tail append merely
because its key is numerically large.

## 10. Locating an insertion position in document order

The target position is derived from document structure, never from key order.
For a new projected record, the writer identifies the closest preceding live
record in the same projection record set. The allocation-free raw-node helpers
`nearestPreviousRecord` and `previousDocumentNode` do this as follows:

1. Read the new record's structural node by stable key.
2. Follow its left sibling to that subtree's rightmost descendant, or follow
   its parent when it has no left sibling.
3. Resolve each candidate through the projection record-root definition.
4. Stop at the first live root in the same projected record set that is not
   part of the structurally moved interval.
5. Use that predecessor's stable record key as the positional anchor.

The predecessor record is then found in the projection through its locator or
normal fence plus exact `KEYS` search. The new row is spliced immediately after
that physical row.

For a first record there is no predecessor, so the insertion is anchored at
the document head. For several records inserted in one transaction, apply-time
planning recomputes every predecessor/successor edge from the current document
structure and builds a primitive `nextInsertion` chain for that maintenance
pass. The listener retains keys and structural provenance; it does not retain a
detached insertion-chain object. The planner cannot sort new keys, because
allocation order and final document order may differ within a compound
operation.

The fallback walk is CPU-proportional to intervening unprojected nodes, but it
does not allocate. It fails closed after 1,048,576 traversed nodes rather than
becoming unbounded. A future order label may optimize this traversal; it is not
required for correctness.

## 11. Local row-group splits

A row group holds at most 1024 rows. Inserting into a full group performs a
local split:

1. Read the full source row group.
2. Merge the new row at its document position.
3. Emit contiguous document-order slices. A one-row insertion produces two;
   a same-commit batch may require more than two.
4. Keep the first slice in the existing physical slot.
5. Allocate or recycle the additional physical slots.
6. Link those slots immediately after the source in document order.
7. Recompute normal fences only for the emitted slots.
8. Update sparse locators only for exceptional rows whose physical slot
   changed, plus genuinely new exceptional rows in the batch.

At most 1024 pre-existing rows can cross a source split boundary, so rewrites
of their locators are bounded by one source row group's capacity. New locator
writes are additionally bounded by the number of newly inserted exceptional
records. Normal rows have no locators to update.

The split does not shift later slot numbers and does not rewrite distant row
groups. Physical document-order links, not numeric slot adjacency, make this
possible.

An insert changes row membership and ordinals, so every projected column in
each touched source/destination slice is rebuilt, just as for delete. The bound
is the local emitted row groups, never the complete projection.

## 12. Physical order and normal routing are separate structures

Each live physical row-group slot carries document-order links:

```text
document previous slot
document next slot
owner base slot
```

Initial-build row groups become stable base/sentinel heads. Locally allocated
split row groups belong to one base chain. A normal-only skip structure links
the members of a base chain that contain normal rows. Exception-only row
groups remain in physical document order but are invisible to normal numeric
routing.

The base high-water boundary is monotone:

- A delete does not lower it.
- Demoting a moved normal row does not lower it.
- Only a proven normal tail append may raise the last base boundary.

This may leave conservative routing ranges after deletes or moves. That is
intentional. Exact `KEYS` verification distinguishes a real row from an empty
range hit, while keeping ordinary maintenance local.

## 13. Record moves

Consider moving record key 8 before record key 2. Physical document order
becomes:

```text
8, 2, 5
```

The moved record can be demoted to an exception without reclassifying the
unmoved records:

```text
physical order: 8, 2, 5
normal backbone:   2, 5
exception:       8
```

The maintenance operation:

1. Captures the old record-set membership and old positional context before
   the structural move.
2. Captures new membership and new positional context after the move.
3. Locates the source projection row by stable key 8.
4. Removes it from the source position.
5. Locates the new predecessor or document head.
6. Inserts the same stable record at the target position.
7. Adds or updates locator `8 -> target physical slot` and sets its exception
   bit. Ordinary maintenance conservatively classifies every pre-existing
   record moved within the same projected set as an exception, even if the move
   crosses only rows that were already exceptions.
8. For a one-record move with space in the target, patches only the source and
   target row groups plus bounded link/fence metadata. A same-row-group move
   rewrites one row group. A full target may split locally, and a moved subtree
   containing `N` projected roots performs the same bounded work for those
   `N` rows and their affected source/target groups.

Moving a subtree into or out of the projection's record set is a membership
change as well as an order change. The before/after structural notifications
must distinguish:

- pre-existing record moved within the same set;
- record moved out of the set;
- record moved into the set;
- genuinely newly created record;
- record deleted from the document.

This provenance is necessary because an absent projection lookup is not enough
to prove that a record is new.

A move that changes row order likewise rebuilds `KEYS` and every projected
column inside its touched source/target row groups. Columns move together with
their row; a value-only column patch is not sufficient for positional change.

Exceptions are not promoted during ordinary maintenance merely because a
later move would allow them to become normal. Avoiding opportunistic promotion
keeps mutations local and makes locator lifetime deterministic. An explicit
initial build may classify rows greedily into the normal backbone.

## 14. Sparse locator wire contract

For non-negative `recordKey`, the locator key is:

```text
Long.MIN_VALUE | recordKey
```

The live locator value is exactly five bytes:

```text
byte 0:      format version 0
bytes 1..4: physical row-group slot, little-endian int
```

A zero-length value is a tombstone. The locator is stored as a raw inline HOT
value. It must never be sent through the projection blob/overflow path:

- Five bytes do not justify an overflow page.
- A negative locator owner key is deliberately outside the non-negative data
  slot namespace used by referenced projection segments.

Unbounded HOT scans that discover non-negative projection data slots must skip
negative locator keys before decoding a row-group/segment composite key.

## 15. Fail-closed lookup and mutation rules

Projection maintenance is part of commit correctness. Inconsistent metadata
encountered by the lookup, touched-unit validation, or physical-order reader
must abort the owning transaction before publication. Ordinary commits do not
perform a global validation scan merely to prove that distant metadata is
healthy.

The implementation rejects at least these states:

- A locator value has the wrong length or unknown version.
- A locator names an out-of-range or non-live physical slot.
- A locator's target `KEYS` does not contain the requested record key.
- A locator's matching row lacks the exception bit.
- A row has an exception bit but no matching locator when that exceptional row
  is looked up or its row group is being rewritten.
- An exact lookup or touched row group encounters the same record key more than
  once.
- A normal lookup finds a row marked exceptional.
- Normal fences overlap or violate their increasing backbone invariant.
- A physical-order read or local splice encounters disagreeing previous/next
  links or a cycle.
- A physical-order read or local splice reaches a free physical slot.
- A dirty pre-existing record has no row in the prior projection snapshot.
- A missing row is inserted without proof that the record is genuinely new or
  moved into the record set.
- A deletion notification cannot be attributed while the old parent chain is
  still readable.

Returning "not found" is correct only when absence is a valid result for the
caller's provenance. In particular, a missing exception locator becomes a
commit error when caller provenance proves that a pre-existing dirty record
must already have a projection row; it must not be converted into an apparently
clean insertion or absence.

## 16. Incremental locality contract

Ordinary commits never rebuild the full projection and never invoke the HOT
writer's arbitrary-subtree reconstruction path. A split follows these bounded
phases:

1. Select the directly affected leaf, or the smallest complete
   flattened-BiNode frontier containing the two adjacent routing slots.
2. Require every frontier member to be a direct leaf. The frontier is therefore
   hard-capped by one HOT node's fanout; it never descends into or materializes
   an arbitrary child subtree.
3. Rebuild only those leaf contents. The common case is two sibling leaves; a
   valid flattened shape may require three, or at most that parent's direct
   leaves.
4. Recompress the surviving parent mask, dropping discriminator columns that
   became constant, and re-encode only the ancestor spine needed to reconnect
   it.
5. Reattach every referenced projection segment by owner-slot key.
6. Validate ranges, routing, height, parent masks, and segment owners before the
   path-reference swap that publishes the new structure.

If anything unexpected fails after that publication boundary, the page
transaction becomes rollback-only. The first failure remains authoritative;
further HOT mutations and commit are rejected; disposable fresh leaves are
reclaimed; and only rollback may install a clean writer. A partially propagated
trie cannot become a committed revision.

The intended persistent work, in phone-readable form, is:

- **Value update:** selected column segments, one descriptor, slot-0 metadata,
  and bounded derived column units when applicable.
- **Delete:** each affected source row group, its changed local fence/link unit,
  an exception locator if applicable, and slot-0/derived metadata.
- **Insert without split:** each target row group, its changed local fence/link
  unit, one locator per new exceptional row, and slot-0/derived metadata.
- **Insert with split:** each affected source and locally required destination
  group, changed local links/fences, crossed and new exception locators, and
  slot-0/derived metadata.
- **Same-group move:** one touched row group when all moved rows share it and no
  split is required, plus changed local locator/fence and slot-0/derived units.
- **Cross-group move:** affected source and target groups, including local split
  destinations, plus changed local locator/fence/link and slot-0/derived units.
- **Unrelated change:** no projection bytes.

Small metadata is chunked so changing one link or fence does not rewrite a
global fence array. Distant row groups and unrelated projection definitions
remain untouched.

Validation is local too. For every touched physical leaf, the maintainer checks
its immediate document-order predecessor/successor reciprocity in constant
time. A normal leaf's numeric interval is checked against the preceding normal
base boundary and its level-0 normal successor; an inserted split is found
through the existing bounded-height numeric skip path. Exception-only leaves
do not participate in numeric routing, so validation never walks across a long
run of exceptions looking for a distant normal sibling. For example, the
regression with 2,048 exception-only leaves between normal leaves validates a
touched middle slot by reading exactly its two local metadata chunks, rather
than scanning the intervening leaves.

No operation resets a populated projection subtree. Initial construction is a
distinct, hard-guarded initializer that can publish only into a physically
virgin tree. After publication, all inserts, updates, deletes, and moves use
the incremental route above. An exception or inconsistent persistent unit
makes the owning transaction fail; it never falls back to a rebuild.

Drop removes the catalog definition and leaves the historical physical tree
untouched. Replacement is drop + commit + create, and create allocates a fresh
physical index id before running the virgin initializer. Definition-id reuse
and in-place repair are not lifecycle operations.

## 17. Versioning contract

Projection storage uses Sirix copy-on-write pages and must be correct under all
four `VersioningType` values:

- `FULL`
- `DIFFERENTIAL`
- `INCREMENTAL`
- `SLIDING_SNAPSHOT`

The projection algorithm emits changes at semantic slot/segment granularity.
The configured versioning algorithm decides how the containing versioned pages
are reconstructed and shared, but it must not change projection semantics.

For every type, tests must prove:

- The committed revision sees the update, delete, insert, or move.
- The preceding revision retains its old `KEYS`, values, locators, fences, and
  document order.
- Cold reopen reconstructs both revisions identically.
- Tombstones hide removed locators and removed segments only in the new
  revision.
- A page-reference or record version is not confused with the V0 projection
  wire-format byte.
- Revision numbers, node keys, physical slots, and version counters retain
  their declared types and are never narrowed accidentally.

`FULL` may have different underlying page-write behavior from a delta-based
versioning type. That is not permission for the projection maintenance layer
to perform a complete logical index rebuild.

## 18. Allocation and HFT contract

Correctness and locality must not be bought with transaction-sized allocation.
The common ingestion and update paths obey these rules:

- No per-row boxed objects in encode, lookup, or maintenance loops.
- No exception bitmap allocation for a normal-only row group.
- No exception bitmap decode/copy for a value-only update.
- At most 128 bytes of live bitmap payload for an exception-bearing 1024-row
  group: 16 longs, plus the JVM array header when materialized.
- A membership rewrite may use one primitive `BooleanArrayList` for the touched
  group's flags (at most 1024 booleans, roughly 1 KiB of payload). It does not
  allocate boxed booleans or clone a bitmap once per row/update.
- Reuse primitive row-group, column, and codec scratch buffers.
- Do not build a locator entry for every record; locators are sparse.
- Do not retain all encoded row groups during an initial build; stream bounded
  row groups into persistent storage.
- Collect an exact `STRING_SET` summary for a column only while its complete
  encoded value/count set fits both the configured value-count and byte limits.
  The first unseen value that would exceed either limit permanently disables
  that column for the build and immediately clears its primitive count map.
- Stream Bloom references in 256-leaf chunks. Persist every full chunk eagerly
  and retain only the current incomplete chunk before the metadata manifest is
  published.
- Do not sort dirty/new record keys to recover document order.
- Do not retain deleted source subtrees.
- Bound pre-existing-row split work by the 1024-row source group; same-commit
  batches add work proportional only to their new rows and local destination
  groups.
- Bound fence/link rewrites by the touched metadata chunks.

The HFT gate remains an end-to-end runtime requirement: no major, old, full,
mixed, humongous, or allocation-failure GC, and no foreground, safepoint, GC,
or drain pause above 250 ms in the measured window. Async-profiler CPU and
allocation profiles diagnose hot paths; they do not replace the GC/safepoint
gate.

## 19. Exact revision walkthrough for the running example

The three revisions have these logical projection states:

### Revision 1: initial build

```text
KEYS:       [2, 5, 8]
id:         [1, 2, 3]
price:      [10, 20, 30]
exceptions: none marker
locators:   none
fence:      slot 1 -> [2, 8]
```

Lookup 5 selects slot 1 through the fence and verifies `KEYS[1] == 5`.

### Revision 2: update record 5 price

```text
KEYS:       [2, 5, 8]       semantic slot not rewritten/logically unchanged
id:         [1, 2, 3]       semantic slot not rewritten/logically unchanged
price:      [10, 25, 30]    new price segment
exceptions: none marker     unchanged, never materialized as a bitmap
locators:   none
fence:      slot 1 -> [2, 8]
```

Revision 1 still returns price 20. Revision 2 returns price 25.

### Revision 3: delete record 5

```text
KEYS:       [2, 8]
id:         [1, 3]
price:      [10, 30]
exceptions: none marker
locators:   none
fence:      slot 1 -> [2, 8]
```

Revision 2 still contains record 5. Revision 3 does not.

Notice that `[2, 8]` still legitimately has fence `[2, 8]`; a fence is a range,
not a promise that every integer inside it is present. Looking up deleted key 5
may select slot 1, but the exact `KEYS` search finds no match and reports a
valid absence for a deletion-aware caller.

## 20. Required test matrix

The implementation is incomplete until focused tests cover:

### Exact lookup

- One normal record in a row group.
- First, middle, and last normal records.
- Numeric gaps such as `2, 5, 8`.
- A fence hit with no exact `KEYS` match.
- First, middle, and last exceptional records.
- Locator/bit/KEYS disagreement failures.

### JSON mutations

- Value-only update of each supported column shape.
- Delete first, middle, last, and only record.
- Insert first, middle, and tail.
- Multiple inserts in one commit whose key order differs from document order.
- Move left, right, across row groups, and within one row group.
- Move into and out of the projected record set.
- Nested projection roots with unprojected nodes between records.

### XML mutations

- Projected element/attribute value update.
- First/middle/last insert and delete.
- Subtree move within, into, and out of the record set.
- Text-node coalescing cases that emit compound structural notifications.

### Boundaries

- Row counts 0, 1, 63, 64, 65, 1023, and 1024.
- Insert into a full row group and verify a local split.
- Delete a local split group to empty and recycle it.
- Empty immutable base/sentinel row group.
- Exception-only row group.
- All-normal marker without bitmap allocation.
- Split with exceptions on both sides and exact locator rewrites.

### Versioning and persistence

- Every mutation scenario under all four `VersioningType` values.
- Query old and new revisions before close and after cold reopen.
- Drop/recreate allocates a fresh physical id and proves that locators in the
  historical tree cannot enter the replacement.
- Malformed/future locator and KEYS markers fail closed.
- Storage scans never decode negative locator keys as data slots.

### Performance

- Value-only update writes no KEYS/exception bitmap segment.
- Ordinary normal LDJSON ingestion creates no locator entries.
- Local insert/delete/move leaves distant row-group hashes unchanged.
- Allocation profile confirms no per-row allocation regression.
- CPU profile identifies bounded local maintenance rather than global scans.
- HFT GC and pause gates remain clean.

## 21. Implementation map

The principal implementation responsibilities are:

- `ProjectionIndexChangeListener`: change provenance, record-root resolution,
  positional mutation planning, and fail-closed commit maintenance.
- `ProjectionIndexRowExtractor`: exact extraction of selected columns from a
  source record root.
- `ProjectionIndexRowGroupPage`: primitive in-memory rows and lazy exception
  metadata.
- `ProjectionIndexColumnSegmentCodec`: V0 KEYS marker/bitmap encoding and
  independent column-segment encoding.
- `ProjectionRecordLocator`: sparse negative-key locator codec and access.
- `ProjectionPersistedRecordLookup.find/keys/exactMatch`: locator-first and
  fence-fallback routing, verified descriptor/KEYS loading, and exact unique
  row/exception-bit validation.
- `ProjectionIndexFences`: persistent physical document links, immutable base
  boundaries, and normal-only routing.
- `ProjectionIndexHOTStorage`: disjoint raw locator and non-negative
  data/blob-slot access, including scan namespace checks.
- `ProjectionIndexBuilder` and `ProjectionBulkLoad`: streaming initial build,
  greedy normal/exception classification, and locator persistence.
- `ProjectionIndexMetadata`: the V0 metadata/versioning contract, physical-slot
  counts, revision binding, and derived-unit references.
- `AbstractHOTIndexWriter.tryDirectionOneLeafPairSplice`,
  `HOTIncrementalInsert.minimalBiNodeRangeContaining`, and
  `HOTIncrementalInsert.replaceChildRangeAndCompress`: smallest-complete-frontier
  HOT replacement without arbitrary-subtree reconstruction.
- JSON and XML index controllers/transactions: before/after structural
  notifications with enough provenance to distinguish create, delete, and
  move.
- `PinnedTrieProjectionSpillColdReopenTest.arbitraryOrderMaintenanceRemainsIncrementalAcrossColdHistory`
  and
  `ProjectionIndexXmlIntegrationTest.siblingMaintenanceStaysIncrementalForEveryVersioningType`:
  insert/update/delete/move and cold-history proof under every
  `VersioningType`.

The core invariant tying these classes together is simple:

> Every live projected record appears exactly once in physical document order;
> its explicit `KEYS` value is the authority for row identity; and exactly one
> validated routing path, normal fence or sparse exception locator, reaches it.
