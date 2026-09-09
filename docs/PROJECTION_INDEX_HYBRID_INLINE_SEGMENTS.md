# Projection Index — Segment-Slot Inline / Overflow Storage

> **Current format.** A row-group descriptor contains metadata and zone maps
> only. Every encoded segment has exactly one HOT segment slot. A segment payload
> of at most **512 bytes** is stored inline in that slot; a larger payload is
> stored in an `OverflowPage` referenced by that slot.
>
> An earlier design proposed putting small segment payloads in a trailing region
> of the row-group descriptor. That proposal was rejected and is documented here
> only to explain the motivation for the current placement. Descriptor-inline
> payloads are **not** a supported historical, compatibility, test, read, or
> write format. Readers must reject them; writers must never emit them.

For a less technical walkthrough, see
[`PROJECTION_INDEX_HYBRID_EXPLAINED.md`](PROJECTION_INDEX_HYBRID_EXPLAINED.md).
The broader index layout is described in
[`PROJECTION_INDEX_STORAGE_REDESIGN.md`](PROJECTION_INDEX_STORAGE_REDESIGN.md).

## 1. The one supported layout

Each row group has one descriptor slot and one slot per encoded segment:

```text
(rowGroupId, 0)               zone-map-only row-group descriptor
(rowGroupId, segmentId + 1)   that segment's HOT slot
```

The descriptor records the row count, key range, column kinds, and each
segment's identity, encoded length, content hash, provenance, and min/max zone
map. It contains no segment payload and no trailing payload region.

Each segment slot independently uses the same fixed placement rule:

```text
encoded segment length <= 512 bytes  -> payload inline in its own HOT slot
encoded segment length >  512 bytes  -> slot references one OverflowPage
```

The threshold is part of the storage contract, not a per-query or benchmark
knob. There is no per-row-group byte budget and no competition among sibling
segments. In this document, **inline** always means inline in the segment's own
HOT slot, never inline in the descriptor.

This gives every segment one logical address and one authoritative byte source:

- for a small segment, the bytes are the segment-slot value;
- for a large segment, the bytes are in the `OverflowPage` referenced by that
  segment slot;
- the descriptor never contains a second copy.

## 2. Why small segments should be inline

A projection divides a row group of at most 1024 rows into independently encoded
segments: KEYS, one BODY per projected column, and a DICT where a string column
needs one. Many real segments are tiny:

| Example segment | Typical encoded size |
|---|---:|
| Empty or constant BODY | 7–33 B |
| Small string dictionary | 16–60 B |
| Boolean BODY for 1024 rows | about 152 B |
| Dense ascending KEYS | about 160 B |

Putting each such blob in its own page adds page framing, a reference, a cache
entry, and another read. Keeping a payload of at most 512 bytes in its own HOT
slot avoids that overhead and improves locality.

Large numeric bodies, high-cardinality dictionary bodies, and FSST dictionaries
can be several KiB. They belong in `OverflowPage`s so HOT leaves stay compact
and split cheaply. The 512-byte boundary therefore preserves both properties:
small-segment locality and bounded HOT-slot size.

## 3. Why descriptor-inline was rejected

The retired proposal put selected small payloads after the descriptor's entry
table. It used a storage-class bit in an entry, a per-segment eligibility limit,
a total inline budget, and a packing order. Although that removed pages for tiny
segments, it was the wrong ownership boundary:

- changing one inline segment also changed the descriptor value;
- multiple sibling payloads competed for a shared budget;
- descriptor density and split cost depended on the combined payloads;
- readers and writers needed two persisted segment locations;
- migration between descriptor-inline and referenced storage complicated
  incremental maintenance and corruption handling;
- accepting both layouts would create two wire formats and two mutation paths.

The segment-slot layout keeps the useful size-based decision but makes it once,
locally, for each segment. A small segment shares a slot with nothing else. A
change cannot force an unrelated sibling to move, and descriptor reads remain
small and predictable.

The rejected proposal never defines a readable compatibility format. In
particular, a descriptor entry that claims to contain inline bytes, or a
descriptor with trailing segment bytes, is malformed under the current format.
It must fail validation rather than be converted, stripped, migrated, or
silently accepted.

## 4. Read path

To read a segment:

1. Read and validate the zone-map-only descriptor entry for its expected
   length, hash, and provenance.
2. Resolve the segment's own HOT slot.
3. Read the payload directly from that slot when it is inline, or follow its
   reference to an `OverflowPage` when it is not.
4. Verify the payload against the descriptor's expected length and content
   hash.

The descriptor supplies integrity metadata and pruning information; it is not a
fallback payload source. A missing segment slot, an invalid slot kind, an
unexpected descriptor-inline marker, or a length/hash mismatch is corruption
and must fail closed.

## 5. Incremental write path

For a touched row group, encode the affected logical segments and compare each
result with the previous descriptor entry:

```text
same encoded length and content hash
    -> preserve the existing segment slot and any OverflowPage reference

changed encoded bytes
    -> update only that segment's HOT slot
       <= 512 B: put the payload in the slot
       >  512 B: put the payload in a new OverflowPage and reference it

changed zone map or segment identity
    -> update the row-group descriptor slot
```

Untouched row groups and unchanged sibling segments are carried forward. An
inline-to-overflow or overflow-to-inline transition changes only the segment's
own slot (plus descriptor integrity metadata when its length or hash changes).
There is no whole-index rebuild and no descriptor payload repacking.

The HOT descriptor and segment slots participate in the resource's configured
`VersioningType`. An `OverflowPage` is an immutable, offset-addressed payload:
an unchanged reference is shared, while a changed large segment receives a new
page. This is intentional copy-on-write behavior, not a second index format.

## 6. Split and recovery invariants

The following invariants are required for every supported versioning strategy:

- A descriptor is always zone-map-only.
- Each descriptor entry has exactly one corresponding segment slot.
- A segment slot has exactly one payload representation: inline or overflow
  reference.
- A HOT split moves descriptor and segment slots by their logical keys; it does
  not unpack or repack sibling payloads.
- Sparse fragments must retain any dirty slot whose value or resolved reference
  changed.
- Recovery must never synthesize a segment from descriptor bytes.
- Validation failures are reported; unsupported bytes are not auto-migrated.

These rules keep update containment at one row group and, within that row group,
at the smallest changed segment slots.

## 7. Worked placement example

Suppose a row group produces these segments:

| Segment | Encoded bytes | Placement |
|---|---:|---|
| KEYS | 900 B | segment slot references `OverflowPage` |
| BODY(age) | 1,600 B | segment slot references `OverflowPage` |
| BODY(active) | 152 B | inline in BODY(active)'s segment slot |
| BODY(dept-id) | 384 B | inline in BODY(dept-id)'s segment slot |
| DICT(dept) | 60 B | inline in DICT(dept)'s segment slot |

The descriptor contains five fixed entries and their zone maps, but none of
those payload bytes. Three independent HOT slots contain inline payloads; two
independent HOT slots reference overflow pages. Updating `BODY(active)` touches
its slot and the descriptor only if its recorded integrity or zone-map metadata
changes. It does not copy or relocate the other four segment payloads.

## 8. Terminology and non-goals

- `OverflowPage` is the sole external payload page type for a projection
  segment; there is no projection-specific duplicate page class.
- The layout does not add row-level deltas inside an encoded segment. A changed
  segment is re-encoded as the bounded maintenance unit.
- The layout is placement, not compression. It removes overhead around small
  encoded payloads; it does not change their codec.
- There is no descriptor-inline escape hatch, compatibility reader, benchmark
  mode, or test-only persisted variant. Tests that need large-segment behavior
  must use a payload larger than 512 bytes and exercise the same production
  format.
