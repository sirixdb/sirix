---
kind: note
---
### Logical codec bounds

When codec input uses reusable scratch, test every truncated frame length while
leaving the complete valid frame in the backing array. Run the same adversarial
contract against every output overload: bounds against array capacity alone can
silently consume stale bytes. Require exact input consumption and bound decoded
output before any writes. See the [native zero-run postmortem](postmortems/2026-11-17-zero-run-native-frame-bounds.md).

### Cold paths can change warm compilation

When adding a cold codec, include its setup work in fresh-process warm lookup
benchmarks. Heap-backed foreign-memory scalar accesses can alter process-wide
receiver profiles and slow unchanged native lookup methods. Keep heap parsing
on array accessors and native FFM paths native-only; verify compiled shape as
well as allocations. See the [leaf-codec postmortem](postmortems/2026-11-17-leaf-codec-receiver-profiles.md).

### Measure allocation at each cold entry point

A zero-allocation warm lookup does not establish an allocation-free codec.
Compare raw and encoded arms for encoder selection, decode into existing storage,
full/compact materialization, image creation and uncached selection. Reuse input
arrays **and** parsing cursors. A native view eliminated in a standalone
helper may still allocate in a larger caller; use JFR stacks to verify whole
entry points and remove avoidable view construction instead of assuming escape
analysis will remove it. When instrumentation changes the result, disable escape analysis for the allocation-site comparison. Gate every fresh fork, rather than only the mean. Pass owning base segments and primitive offsets through an adapter instead of replacing checked FFM arguments with raw addresses. Compare unchanged main with owned-result fixtures. When substituting scratch for an exact array, test every logical
truncation after a valid longer frame and verify previously published images
remain independent. See the [codec allocation postmortem](postmortems/2026-11-17-leaf-codec-allocation.md).

### Move conditional native resources with their loader

A loader refactor must update native-image `typeReached` resource conditions as
well as Java calls. JVM tests do not apply those conditions. Pin the resource's
actual loading type in a packaging regression, and keep downcall descriptors in
sync with adapter signatures. See the [resource reachability postmortem](postmortems/2026-11-17-native-codec-resource-reachability.md).

### Production read ownership and retention

A direct decoder comparison can hide a production regression when raw reads
transfer decompression storage and encoded reads allocate a second destination.
Exercise the public reader as well as the real byte-handler ownership route;
remove an identity-pipeline copy when the decoder always owns its output. Assert live allocator bytes and
cache charge, then force mutation and verify lazy promotion invalidates the old
stamp and releases its allocation. Include failure after decode, such as a
truncated side-reference trailer. See the [retained-frame postmortem](postmortems/2026-11-17-leaf-codec-retained-frames.md).

### Codec preference is a separate boundary

A smaller encoding does not automatically justify its decoder, and a codec's name
does not say what decoding costs. The leaf-codec plan called zero-run the cheap
codec and a review round built a margin on that; timed on real leaves, zero-run
decoded 3.3 times slower than native LZ4 and saved less. Before a rule prefers one
codec, time every decoder a store can meet (native and fallback) on real payloads
through the production reader, and derive the rule from those costs and the cost
of reading a byte. Test one step under, on and over the rule's boundary with
framing included, measure codec lengths independently and assert the emitted
codec as well as the round trip. Keep reader tests for every codec id supported by
the current format, on frames the test builds, so they do not depend on what the
writer chooses on that host. See the
[preference-margin postmortem](postmortems/2026-11-17-leaf-codec-preference-margin.md).

### Codec CI contracts

Keep exact packing oracles on decoded logical frames and checksum oracles on stored wire
frames. Compression fixtures used across platforms must compress with every supported
codec set; explicitly exercise missing-native-codec behavior. Verify native staging names,
fallback directories and losing-publication cleanup through filesystem behavior, and
separate functionally equivalent decoder-routing mutants from missing correctness tests.
Keep encoded-frame expanders strict when callers already route raw frames: accepting raw
scratch adds a redundant copy route that functional assertions cannot distinguish. Upload
reports from all test tasks, including allocation forks, and print complete failures there.
See the [leaf codec CI postmortem](postmortems/2026-11-17-leaf-codec-ci.md).

- **Do not depend on escape analysis to remove codec accessor objects.** A heap segment's
  `heapBase()` returns an `Optional` per call. Reuse the writer's cached buffer view when
  accessing its heap array, and validate allocation gates on HotSpot with escape analysis
  disabled as well as the workstation compiler. Exercise both process settings through
  the shared writer, and document intentional thread-lifetime scratch at the field.
  Evidence: [leaf codec CI follow-up](postmortems/2026-11-17-leaf-codec-ci.md#follow-up-hotspot-allocation-writer-mode-coverage-and-sonar-lifetime).

### Runtime vector width is an allocation boundary

A fixed species' lane count does not establish hardware support. Gate fixed-width
Vector API operations on runtime-preferred width across every primitive representation;
otherwise software fallback can allocate on each cached lookup. Run exact allocation
bounds in a clean narrow-vector C2 fork as well as the normal runtime, with unsigned
key-order oracles for both lookup overloads. Attribute equal raw/encoded allocations
to their actual stack before calling them codec overhead. See the
[HOT leaf vector-width postmortem](postmortems/2026-11-17-hot-leaf-vector-width.md).

### Verify ordered VM options on the CI runtime

A compiler-specific regression fork must start on the CI JDK as well as the local
JDK. Unlock experimental VM options before setting `UseJVMCICompiler`, even when
disabling it: GraalVM can accept an argument list that Temurin rejects. Verify a
real Gradle worker and its existing behavioral assertions on the CI distribution;
removing or misordering the unlock must fail startup. Keep compiler/vector flags
and strict allocation bounds intact. See the
[fork-startup postmortem](postmortems/2026-11-18-hot-leaf-fork-startup.md).

### A closed check is not a lifetime

Code that reads off-heap page memory must hold the page for the whole read, or validate a
stamp afterwards and discard what it read. Checking `isClosed()` first proves nothing about
the bytes read next: eviction can release the frame in between, and a released frame keeps
its bytes until it is reused, so the read is usually still correct and the defect hides
until a slower or busier machine reuses the frame in time. Zero-fill released frames
(`FrameSlotAllocator.setPoisonOnReleaseForTesting`) when testing any such reader, evict at
the point between "resolved" and "read", and assert on background threads as well as the
test thread. See the
[validator leaf-lifetime postmortem](postmortems/2026-11-18-hot-validator-unguarded-leaf-read.md).

### Fewer bytes only pay where the bytes come from the device

- A page codec was credited with "cold reads only: a decode replaces a copy". In the gate's cold
  regime a fresh process reads from a warm page cache: the smaller pages save nothing there, every
  loaded leaf is decoded, and the first use of the decoder costs about a millisecond once.
- Before building a codec, multiply the per-page decode cost by the pages a fresh process loads for
  the heaviest gated query, and compare it with that query's time. If it exceeds the gate's
  threshold, the codec is opt-in from the start.
- Measure in the gate's own regime first. A half-hour run of the real cold endpoints with the
  feature on and off would have shown this before the microbenchmarks, the device probe and the
  image builds. A device-cold probe and an in-memory decode comparison answer different questions.
- See [`2026-11-18-leaf-codec-default-cold-cost.md`](postmortems/2026-11-18-leaf-codec-default-cold-cost.md).

### Corrupt wire fixtures must pin their layout

Build ordinary and packed golden controls independently of the writer's
selection policy. Use the declared directory width for both the stride and
the offset load. After editing a valid fixture, assert at the reader
boundary so a failure in fixture construction cannot stand in for a
decoder refusal. See the
[projection directory fixture postmortem](postmortems/2026-11-19-hot-leaf-directory-fixture-width.md).

### Check logical bounds before packing

Source slots carry a logical high-water mark separate from allocator capacity.
Before choosing or emitting a packed layout, check each selected header and
value against the used byte range. Exercise retirement and sparse emission
with a real backing allocation larger than the logical range. See the
[packed HOT writer postmortem](postmortems/2026-11-19-hot-leaf-packed-source-bounds.md).

### Reuse unpublished packed wire views

Borrowed slot selection keeps only a synchronous descriptor with its backing
cleared on every exit. A copying reader can rebase its unpublished descriptor
to the destination. Count the descriptor, backing wrapper and slices in the
clean allocation fork, including formats that save enough wire bytes to hide
the object costs in other read paths. See the
[packed HOT allocation postmortem](postmortems/2026-11-19-hot-leaf-packed-read-allocations.md).

### Preserve contiguous keys for fixed-word probes

Select grouped prefixes only where the read path can afford two key
ranges. Preserve the bounded native word lookup for eight-byte projection
keys, including the last slot where an otherwise safe forward word load
can cross the slot region. Keep the suffix-work counter active on packed
paths. See the [projection probe postmortem](postmortems/2026-11-19-hot-leaf-projection-suffix-budget.md).

### Distinguish writable copies from cache images

Preserve the mutable-copy contract when a cache starts retaining encoded
pages. Give the cache an explicit read-only copy path and charge all
retained wire metadata. Check that independent copies can grow and
mutate while the guarded source retains the same values. See the
[packed copy postmortem](postmortems/2026-11-19-hot-leaf-packed-copy-contract.md).

### Let pointers name the directory entry

When a slot owns shared bytes, make every alias identify that slot with
one directory load. The validator can then check earlier ownership and
reuse proved bounds without an additional search. Prove the current
slot's span before reading its alias. Reuse an earlier alias proof only
when both the target and its declared length match. Measure complete
and compact materialization, borrowed selection and retained hits
separately. Check the compiled borrowed loop when replacing a typed
load with narrower loads. When writer eligibility changes, keep
ownership fixtures on eligible pages and assert the resulting wire mode.
See the [packed read postmortem](postmortems/2026-11-20-packed-hot-leaf-read-cost.md).

### Keep the HOT leaf frame contract across read paths

Exercise a format boundary on full materialization, borrowed point
lookup and cached images. Share the directory and trailer bounds,
then apply the same exact-consumption and trailer-order rules even
when a path skips slot materialization. Pin valid layouts independently
of writer selection. See the [HOT leaf framing postmortem](postmortems/2026-11-20-hot-leaf-framing.md).

### Make size decisions on the logical value

When a stored value gains a compact representation, every decision that
reads its size or bytes must use the logical value: thresholds, hotness,
fold triggers and buffer sizing. Read the logical length and the expanded
bytes through one accessor, and run the full suite, including other
branches' stream tests, before trusting a targeted subset. See the
[packed posting hotness postmortem](postmortems/2026-11-21-hot-packed-posting-delta-hotness.md).

### Bound a decoder by its frame, and give untrusted bytes their own door

A decode boundary that dispatches on a codec id owes the same frame
containment to every codec it can reach: reads bounded by the declared
stored length rather than by the reusable array holding it, and counts and
repeat bytes inside the opcode table. Require exact frame
consumption only where the grammar makes it exact — an encoder that
appends an optional trailing token leaves a legitimately unread byte, so
there the declared size is the postcondition and the envelope owns the
bytes past the stored length.
Hardening one codec and leaving its siblings is not a partial fix but no
fix, because the codec id is attacker-chosen.
Two declared lengths agreeing is not a producible length, and neither is a
declared length with some tokens after it: before reserving anything, require
the declared size to fit what the bytes after the frame header could emit.
Count the opcode bytes when deriving that ceiling — a run's count is
introduced by an opcode, so an N-byte body carries an (N-1)-byte count, and a
ceiling that forgets the opcode admits a gibibyte from five bytes. Let it
climb with the bytes present and stop constraining where a short legitimate
frame really does declare a gibibyte. Keep it a necessary condition and leave
exact production to the decoder, so no second grammar implementation appears.
Bound a running length BEFORE adding to it, against the output still owed and
in a wider type than the length: adding first and testing afterwards survives
only on the headroom an unsigned type happens to have, and the same shape in
signed arithmetic goes negative and passes every later test. Apply it in every
loop a malformed frame can reach, which includes the tail loop a decoder keeps
for its last bytes — for a frame declaring less than the hot loop's slack,
the tail loop is where almost all of its tokens are read.
Where a decoder's hot path deliberately elides input bounds — tail-slack
overshoot, or a native entry point handed a length instead of a capacity —
do not ask its caller to be careful, and do not hand its work to a slower
decoder either: that pays for safety in latency the change was measured not
to spend. Put the bounds inside the fast loop and select them at compile
time, the way LZ4 specialises one decode body into its safe and fast
entry points. One grammar, two specialisations, no drift; resolve the
guarded symbol separately so an older library serves trusted frames instead
of silently losing a refusal.
Decode a valid payload before each malformed one so leaked
scratch is recognisable instead of zeros, and measure a refusal's
allocation against a frame declaring a small size rather than against a
clock. See the
[projection overflow postmortem](postmortems/2026-11-26-projection-overflow-unvalidated-frames.md).

### Charge the allocation, not the view

A cache weight must be the bytes a component will release, not the span it
reads through. The two part company the moment storage is handed out in size
classes or shared with a larger frame: a body sized between two classes is
served the upper one, and a slice of a transferred decompression frame keeps
the whole frame alive. Reporting the view then admits roughly twice the
working set the budget allows, and eviction returns memory nothing counted —
a shortfall no in-process budget can see, because nothing is over its budget.
When the code already carries a field naming the owning allocation — one set
wherever ownership is taken and left null wherever the object owns exact-size
memory — the accounting must read that field rather than re-deriving a size
from the payload. Then check what every producer writes into it: where a
decompression result hands back both a bounded view and the buffer behind it,
the view is what you parse and the buffer is what your releaser frees, so
ownership records the buffer. Reading the right field is only half the fix if
each handoff feeds it the view. Pin such a charge against the allocator's own answer for the
same request; an expectation taken from the object's own accessors agrees with
an under-report on both sides, and a fixture whose size happens to land on a
class boundary hides the rounding entirely. See the
[packed cache-copy postmortem](postmortems/2026-11-26-packed-cache-copy-undercharge.md).
