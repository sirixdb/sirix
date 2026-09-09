# Projection Index Segment Storage, Explained Simply

This is the plain-English companion to
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md).
The precise rule is simple: every encoded segment owns one HOT slot; that slot
holds a payload of at most **512 bytes** directly and references an
`OverflowPage` for a larger payload.

> **There is only one format.** Segment bytes never live in the row-group
> descriptor. A former proposal to pack small segments into the descriptor was
> rejected. It is not an old format that the runtime supports, and readers must
> not accept it.

## 1. The one-sentence idea

> Give every segment its own drawer: keep a small value in the drawer, and put a
> large value in a box with the box's address in the drawer.

This is the same idea as small-string optimization. A short string fits inside
its object; a long string uses heap storage. Here the object is one segment's
HOT slot and the heap object is an `OverflowPage`.

## 2. What is being stored?

A projection index keeps selected JSON fields in column-oriented form. It
divides rows into groups of at most 1024, then encodes several blobs for each
group:

```text
row group (<= 1024 rows)
|- KEYS                    record keys
|- BODY(age)               encoded age values
|- BODY(active)            encoded booleans
|- BODY(department)        encoded dictionary ids
`- DICT(department)        encoded department strings
```

Each blob is a **segment**. A full 100-million-row column is therefore many
small segments, not one giant blob.

The row group also has a **descriptor**. Think of it as a table of contents. It
records which segments should exist, their sizes and checksums, and min/max
values used to skip irrelevant row groups. Those min/max values are often called
zone maps.

The important ownership rule is:

```text
descriptor slot  -> table of contents and zone maps only
segment slot     -> one segment's payload or its OverflowPage reference
```

The descriptor describes payloads; it does not contain payloads.

## 3. Why not give every segment a page?

Many encoded segments are only a few dozen or a few hundred bytes:

| Example | Approximate size |
|---|---:|
| Constant or empty column body | 7–33 B |
| Small string dictionary | 16–60 B |
| 1024 booleans | 152 B |
| Dense record keys | 160 B |

A separate page brings framing, a reference, a cache entry, and another read.
For a 25-byte or 60-byte payload, that bookkeeping is a large fraction of the
total cost. Storing the bytes directly in their segment slot avoids it.

Multi-KiB numeric bodies and large dictionaries are different. Keeping those
outside HOT leaves prevents large slot values from making trie updates and
splits expensive. So the placement rule has one fixed boundary:

```text
0..512 bytes  -> inline in this segment's HOT slot
513+ bytes    -> this segment's HOT slot references an OverflowPage
```

No segment competes with another for space, and there is no per-row-group
packing budget.

## 4. A complete example

Suppose one row group produces:

| Segment | Size | Its own HOT slot contains |
|---|---:|---|
| KEYS | 900 B | an `OverflowPage` reference |
| BODY(age) | 1,600 B | an `OverflowPage` reference |
| BODY(active) | 152 B | the 152 payload bytes |
| BODY(department) | 384 B | the 384 payload bytes |
| DICT(department) | 60 B | the 60 payload bytes |

The layout is:

```text
row-group descriptor slot
    metadata + five segment entries + zone maps

KEYS segment slot
    -> OverflowPage containing 900 bytes

BODY(age) segment slot
    -> OverflowPage containing 1,600 bytes

BODY(active) segment slot
    152 inline bytes

BODY(department) segment slot
    384 inline bytes

DICT(department) segment slot
    60 inline bytes
```

There are two overflow pages, not five. More importantly, the three small
payloads are still isolated from one another. Growing the dictionary does not
force the boolean payload to move.

## 5. Reading a segment

Reading `BODY(active)` works like this:

1. Use the descriptor entry to learn the expected size and checksum.
2. Find `BODY(active)`'s segment slot by its logical row-group and segment id.
3. The slot says that the payload is inline, so use those 152 bytes directly.
4. Verify them against the descriptor.

Reading `BODY(age)` differs only at step 3: its slot refers to an
`OverflowPage`, so the reader loads that page and verifies its 1,600 bytes.

There is no fallback that searches the descriptor for payload bytes. If a
descriptor claims to contain them, it is malformed and opening it must fail.

## 6. Incremental updates

Assume one JSON update flips `active` for one record. Sirix re-encodes the
affected row group's `BODY(active)` segment, then compares its length and
content hash with the previous entry.

- If the encoded result is identical, it writes nothing for that segment.
- If it changed but is still at most 512 bytes, only `BODY(active)`'s slot gets
  the new inline bytes.
- If it grew beyond 512 bytes, only that slot changes to reference a new
  `OverflowPage`.
- Unchanged sibling segment slots and overflow references carry forward.
- The descriptor changes only when its integrity or zone-map information
  changes.

Deletes and inserts use the same production mutation path. They re-encode the
smallest affected row groups and update the changed segment slots; they do not
rebuild the complete projection index.

This remains true across Sirix `VersioningType`s. HOT slot changes use that
resource's versioning rules. Large immutable payloads use copy-on-write: an
unchanged overflow reference is shared, while a changed payload gets a new
`OverflowPage`.

## 7. Why the descriptor-packing proposal was retired

An earlier design sketch put small segment bytes after the descriptor's table of
contents. It needed flags, two size limits, a shared byte budget, and a packing
order. That saved pages, but it tied unrelated segments together: changing one
payload changed the shared descriptor value, and one segment growing could push
another out of the shared budget.

Giving every segment its own HOT slot preserves the useful part—small payloads
avoid pages—without coupling siblings. It also removes format ambiguity. There
is no legacy descriptor-inline reader, migration mode, testing escape hatch, or
second writer. Descriptor-inline bytes are rejected rather than silently
converted.

## 8. What “inline” means everywhere in this design

When current projection-index code or documentation says that a segment is
inline, it means exactly this:

> The complete encoded segment payload is in that segment's own HOT slot and is
> at most 512 bytes.

It never means “inside the descriptor.” Large payloads use the same logical
segment slot, but the slot refers to one `OverflowPage`. This is one storage
format with two size-dependent representations inside the same slot contract,
not two index formats.

For wire-level invariants, validation behavior, and the incremental read/write
contract, continue with
[`PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md`](PROJECTION_INDEX_HYBRID_INLINE_SEGMENTS.md).
