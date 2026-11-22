# NATIVE — Embedded C ABI

## 1. Scope, status and conformance

Spec target: **SirixDB 1.0.0, stable native ABI 1.0**. Adopted design for
[issue I1](https://github.com/sirixdb/sirix/issues/1436), part of the
[native library epic](https://github.com/sirixdb/sirix/issues/1435).
The release target is approximately 23 December 2026; the approximately
9 December disk-format freeze is unchanged. The binding introduces no disk format.
This chapter is the oracle for implementation, not a claim that libsirix exists.
`proposed` records the implementation gap, not an optional release requirement.

Requirements follow RFC 2119/8174 uppercase keywords, stable non-reusable IDs,
and explicit Level, Status, Since, Code, Verified by, Evidence class and Gap fields.
There are 24 requirements. Sections 3 and 4 refine the requirements named in their
headings; they do not create unnumbered obligations. The header is the normative
numeric/signature manifest; this chapter owns semantics. Disagreement is a defect
requiring review, not permission for a binding to choose its own behavior.

### Required 1.0 capability matrix (NATIVE-101, 405, 501)

| Capability bit | Required stable behavior | C | C++ | Rust | Python |
|---|---|---|---|---|---|
| JSON (1) | JSON database/resource, node reads and explicit mutations | Required | Required | Required | Required |
| XML (2) | XML database/resource and complete XML node model | Required | Required | Required | Required |
| JSONIQ (4) | Scoped read-only JSONiq with typed bindings | Required | Required | Required | Required |
| XQUERY (8) | Scoped read-only XQuery with typed XML/atomic results | Required | Required | Required | Required |
| TEMPORAL (16) | Explicit revision, timestamp and bitemporal selection | Required | Required | Required | Required |
| DIFF (32) | JSON semantic diff and XML full diff | Required | Required | Required | Required |
| HISTORY (64) | JSON and XML resource revision history | Required | Required | Required | Required |
| BOUNDED_PULL (128) | Framing, limits, cancel and deterministic cleanup | Required | Required | Required | Required |

The stable capability mask contains all eight bits (`255`); JSON-only is not a
stable 1.0 profile. C++ uses move-only RAII over C, Rust supplies `sirix-sys` plus a
minimal safe thread-affine wrapper, Python supplies typed context managers loading
the library directly. No REST fallback counts as native support. Platform rollout
order is Linux x64, then macOS arm64, then Windows x64; only platform availability
may be staged. Each platform is claimed only after its complete four-language
matrix passes. No listed functional capability may be deferred to 1.x.

Scope excludes static linking, WASM/mobile, hot unloading, runtime Java plugins,
multiple isolates, cross-resource atomic transactions and replacement of REST SDKs.
No existing native CLI/server build is changed. The private AOT runtime includes
its own managed heap and GC; JVM-free deployment does not mean GC-free execution.

## 2. Requirements

### 2.1 Runtime and identity

#### NATIVE-101 — deployment independence

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeNoJvmConformance`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

The supported package MUST load and complete the conformance flow from each of C, C++, Rust and
Python on a clean supported OS without any JDK/JRE installed, no `java` on PATH, no `libjvm`
dependency, no Java child process and no server. Build-time GraalVM is allowed. C++ and Rust
programs link the same C ABI; Python loads it directly.

#### NATIVE-102 — isolate ownership

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeRuntimeLifecycle`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

A process MUST initially have one Sirix runtime backed by one private Graal isolate, shared by
independently owned database handles. `runtime_create` while one exists returns ALREADY_EXISTS; it
does not create an independent global allocator or duplicate per-resource writer registry. The
public facade MUST hide Graal structs and pointers, configure runtime limits before first engine
initialization, and refuse use after shutdown. Graal thread attach/detach and isolate teardown are
implementation details. Multiple loaded copies, loader namespaces and mixing a separate JVM Sirix
engine against the same files in one process are unsupported in this first profile. The packaging
docs MUST say so.

#### NATIVE-103 — thread attachment

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeThreadOwnership`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Each calling OS thread MUST obtain its own context token via `thread_attach`. It is invalid on any
other OS thread; reattach on the same thread returns BUSY until detached. Database/resource handles
may be shared for creation of independent transactions; transaction, query and cursor handles MUST
stay on their creating context. The only cross-context operation on a cursor is `cursor_cancel`,
called through another valid context. Detach with owned live transactions/plans/cursors returns
BUSY. A coroutine or async task may not migrate live cursor calls between workers; bindings pin
their execution or marshal calls to one owner thread.

#### NATIVE-104 — handles

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeHandleStateMachine`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

All exposed identities MUST be nonzero `uint64_t` tokens, never Java object addresses or unvalidated
`ObjectHandle` pointer values. A primitive registry MUST validate runtime epoch, object kind, slot
generation and owner context before access; reuse may not make a stale token valid. Generation
exhaustion retires the slot; runtime epochs cannot wrap into a previously issued token. A parent
must remain live while children exist. Incorrect kind, stale, wrong-runtime and wrong-context inputs
get specific stable errors. This validation is at call/batch boundaries, never per stored row.

### 2.2 Database, transaction and temporal behavior

#### NATIVE-201 — create/open

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeDatabaseContract`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Create MUST fail with ALREADY_EXISTS for an existing database/resource and MUST NOT overwrite it.
Open MUST fail with NOT_FOUND or TYPE_MISMATCH and MUST NOT silently create. Resource names MUST be
one validated logical name, not an arbitrary filesystem path. Paths use length-delimited UTF-8,
converted to native Unicode paths on Windows; embedded NUL and invalid UTF-8 are rejected. Linux
non-UTF-8 byte paths are outside this first ABI. Database aliases resolve through core canonical
ownership. Opening elsewhere while another process owns the database returns LOCKED, including an
attempted read-only open: this is not a multi-process reader facility.

#### NATIVE-202 — repeatable reads

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeRevisionOracle`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Read begin MUST resolve LATEST exactly once, or select an explicit nonnegative revision. Revision 0
is the empty bootstrap. Native revisions are 64-bit at the ABI but values above the engine's
supported signed-int range return RANGE rather than truncate. A cursor's committed view MUST remain
unchanged by later commits. A pruned revision returns REVISION_PRUNED, never another revision. No
implicit cross-resource snapshot is promised.

#### NATIVE-203 — timestamp resolution

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeRevisionOracle`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Timestamp begin MUST capture head H once and select the greatest revision `r <= H` whose commit
timestamp is at/before the requested instant; before all revisions selects bootstrap 0, after H
selects H. Timestamp precision and unique resolution follow
[VER-201–204](VER-versioning-bitemporal.md#ver-201--timestamp-precision-and-persistence), with
checked epoch conversion; valid-time filtering retains Instant precision. An out-of-range
timestamp gets RANGE. The selector returns the resolved revision to the caller, and must not skip
a pruned selected revision. Use `getRevisionNumber(Instant,H)` to preserve the captured ceiling.

#### NATIVE-204 — writer admission

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeWriterAdmission`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

At most one read-write transaction per canonical resource MUST be admitted across all native
database/resource handles. Different resources may have independent writers; no global serial writer
lock is required. First ABI `write_begin` is nonblocking and returns BUSY if occupied. Add a core
try-begin admission seam; a wrapper-only lock does not replace the core lock, and the current
five-second wait is not acceptable for this contract. Reads continue on committed revisions. Write
begin pins the latest revision at actual successful admission.

#### NATIVE-205 — explicit mutations and commit

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeTransactionModel`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Auto-commit MUST be disabled. Mutations operate on a named node key and explicit action (replace
document/subtree, insert first/last child or left/right sibling, remove); wrong node/type/position
errors are deterministic. JSON payload is one complete JSON value; XML payload is a typed node
description (document, element, attribute, namespace, text, comment or processing instruction),
subject to the configured input quota. XML names, namespace bindings, document structure and
insertion position are validated; invalid input returns SYNTAX, INVALID_ARGUMENT or TYPE_MISMATCH as
defined in §3. XML parsing rejects DTDs and external entities. Removal has no payload. No
convenience API that implicitly commits may be used. Reads through the writer see its uncommitted
state; external readers do not. `commit` is synchronous, reports the durable revision, and ends the
native transaction; `rollback` ends it without creating a revision. Successful commit/rollback
consume the handle. A failed NOT_COMMITTED transaction is rollback/close-only; a COMMITTED outcome
consumes it even if cleanup reported an error; UNKNOWN is close-only pending durable recovery
inspection. Neither may proceed while the writer owns an active cursor. Closing an uncommitted
writer rolls it back. No async auto-commit variant is exposed initially.

#### NATIVE-206 — failure outcomes

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeCommitFaults`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Preflight validation failure MUST make no change. A mutation failure after modification starts MUST
mark the transaction failed; only rollback/close remains legal. Commit errors MUST distinguish
NOT_COMMITTED, COMMITTED (with revision) and UNKNOWN outcome. Failure in post-publication cleanup
must not be reported as proof that nothing committed; automatic write retry is forbidden for
UNKNOWN. Failed close invalidates the native token once terminal cleanup is committed, attempts all
independent cleanup actions, and reports the first underlying error in the error record plus a
suppressed-error count; the returned status preserves the consuming-state contract in §3.
Recovery/durability follow the [commit protocol and recovery contract](../docs/DISK_FORMAT.md#1-files);
native code may not discard recovery markers to manufacture a successful close.

#### NATIVE-207 — bitemporal open

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeBitemporalDifferential`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

For both JSON and XML, bitemporal open MUST resolve transaction time by NATIVE-203, pin that
revision and select records valid at `validFrom <= validTime < validTo`. Missing or unparseable
stored bounds preserve the existing engine's unbounded interpretation; this is JSON compatibility
behavior also required of the XML adapter, not a recommendation to write malformed dates. Missing
resource valid-time configuration returns NOT_CONFIGURED. Empty results are normal EOF. Indexed and
scan routes must agree for boundaries, open intervals and historical revisions. The function returns
a streaming cursor, not a mutable transaction.

### 2.3 Queries, serialization, diff and history

#### NATIVE-301 — JSONiq and XQuery

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeQueryDifferential` and `NativeReadOnlyPolicy`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Prepare MUST compile through Sirix/Brackit, not implement another query dialect. The language is
explicitly JSONiq or XQuery and matches the resource model. A read transaction is its pinned context
item and sole allowed resource in profile v1. Typed external bindings are copied at bind time (JSON
value, UTF-8 string, boolean, signed integer, decimal text, instant, XML node); variable names are
expanded namespace/local names, never textual query concatenation. Only one execution per prepared
query may be live. Read queries MUST reject updating expressions and side-effecting Sirix functions
through execution policy, including lazy iteration and workers. Dynamic file/network/module access
is disabled in this scoped profile. This reduces accidental effects; it is not a security boundary
against hostile native code in the same process.

#### NATIVE-302 — pull streaming

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeCursorFraming`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Execution MUST produce a cursor consumed by `cursor_read` into a caller buffer, with at most one
item's fragment per call, item kind and BEGIN/END flags. JSON objects/arrays/scalars use UTF-8 JSON
text; XML nodes use the node-kind framing and namespace/name metadata in §3; non-JSON atomics use a
documented lexical encoding plus an expanded type name obtainable from item metadata. Empty sequence
is EOF, distinct from the JSON `null` item. Serialization may split a multibyte UTF-8 sequence
across chunks; consumers assemble bytes or use incremental decoding. No trailing NUL is written.
Byte count is authoritative. A successful non-EOF read with capacity > 0 MUST either produce bytes
or an explicit empty-item BEGIN|END record; it must not busy-loop with no progress.

#### NATIVE-303 — bounded work and cancellation

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeCursorLifetimeBudget`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Export MUST NOT collect all results or a whole oversized item into a String before returning chunks.
The bridge uses reusable byte/scratch storage, with bounded retained capacity plus serializer depth,
and no foreign callback or extra wrapper/boxing per row. Underlying sorting/grouping/diff operators
may materialize, but all such work has explicit admission/quota and returns LIMIT before exceeding
its budget; pull delivery alone is not a constant-memory query claim. Cursor close/cancel MUST close
the iterator, release pins/readers and drain workers before releasing their storage. Cancel is
cooperative; cancelled cursors return CANCELLED thereafter until closed. No hard real-time deadline
or preemptive kill during durable commit is promised.

#### NATIVE-304 — diff

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeDiffDifferential` against the existing Java JSON/full XML diff and independent patch/replay or changed-node fixtures
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

`diff_open` MUST take one resource and explicit `0 <= old < new` revisions, reject pruned/missing
endpoints, and return the existing JSON semantic-diff vocabulary with correct node keys/array
positions, or XML full-diff events including attributes and namespaces, with declared include-data
mode. Both models use the common cursor and §3 wire schemas. Structural-only XML diff is not parity.
The current BasicJsonDiff tuple list/String implementation is not a bounded streaming
implementation; it needs a budgeted observer/serializer seam or must refuse the request at a
pre-admitted bound. No new JSON Patch compatibility claim is made.

#### NATIVE-305 — history

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeHistoryPaging`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

`history_open` MUST capture head once, return committed revision metadata newest-first over an
inclusive explicit range, exclude bootstrap, and use bounded pages. Metadata contains revision,
timestamp, nullable author/message and retention status. Pruned timestamps must remain enumerable
using the timestamp index; unavailable author/message are explicitly absent, never invented. No
all-revision transaction fan-out. Per-node history is an additive later function using record
history indexes, not a reinterpretation of resource history.

### 2.4 Ownership, errors and ABI

#### NATIVE-401 — buffers and pointer preconditions

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeBufferContract`, C ASan/UBSan wrapper tests
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Every input pointer/length pair MUST be readable for that call, every output writable and
nonoverlapping with input or other outputs. Null is permitted only when length is zero or explicitly
optional. Length overflow and configured maxima are checked before copying. Inputs are never
retained after return; plans own copied query/bind data. All returned bytes belong to caller
buffers; there is no Java-heap pointer, `malloc`/`free` pairing across allocators, or GC pin
exported to clients. Invalid non-null addresses are C caller undefined behavior and cannot safely be
converted into a recoverable error. No zero-copy promise applies to UTF-8 export.

#### NATIVE-402 — close order

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeHandleStateMachine`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Close MUST be explicit. Close child cursors before plans, plans before transactions, transactions
before resources, resources before databases; detach all but the final context, then destroy runtime
through that context. Parent close with live children returns BUSY without changing state; no
surprising cascade. `handle_close(ctx,0)` is a no-op. Closing a previously closed nonzero token
returns INVALID_HANDLE. Separate C resource handles over the same Java session reference-count that
session; only the owner closes it after the last handle/child is gone. Runtime destroy accepts the
sole remaining attached context. Success consumes it; a terminal failure consumes it and poisons the
process runtime guard as specified in §3.

#### NATIVE-403 — errors

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeErrorMapping`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Every fallible function MUST return a fixed-width status, with optional caller-owned versioned error
struct. No Java exception, C++ exception, Rust panic or longjmp crosses the ABI. Export wrappers
catch and classify ordinary exceptions and allocation failures; fatal runtime/native faults may
still terminate the host and are not claimed recoverable. Error storage must be allocation-free for
low-memory reporting. Messages are diagnostic UTF-8, not stable machine APIs or full unbounded Java
stacks. There is no global or thread-local “last error” dependency.

#### NATIVE-404 — ABI stability

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeAbiCompatibility` on each platform and old-client/new-library tests
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Public signatures MUST use fixed-width integers, pointer-plus-uint64 lengths and explicitly
sized/aligned structs; no C `long`, `bool`, compiler enum layout, STL, Java object, Rust
representation or Graal internal type. All public structs, including nested instant values, start
with `struct_size` and `abi_major`; zero-filled reserved fields and unknown flags are checked. New
fields are appended, never reordered. A newer callee accepts a known shorter struct and fills only
caller-advertised output bytes. Calling convention is C/cdecl with C++ `extern "C"`; exported
symbols are `sirix_*`.

#### NATIVE-405 — version/capability negotiation

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeAbiCompatibility`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

`abi_version` and `capabilities` MUST be callable before runtime creation. Stable ABI major is
independent of engine semver and disk-format version; stable symbols remain for the ABI major.
Additive APIs/flags are minor; semantics-breaking changes require a new ABI major. Linux SONAME
`libsirix.so.1`, macOS compatible install name, Windows versioned distribution directory/ABI
metadata. A preview is ABI 0 and must never masquerade as stable ABI 1. Unsupported capability is
reported explicitly, not implemented by invoking REST.

#### NATIVE-406 — terminal runtime

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeRuntimeLifecycle`
- **Evidence class:** partial engine smoke and header evidence in §5; public-ABI runtime conformance unverified
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Destroy MUST stop admission, refuse outstanding user handles/other contexts, drain internal workers,
retire caches and leases, close owned services, release allocator reservations, then tear down the
isolate. Failure must return a documented state (still-live retryable or poisoned terminal), never
free memory still used by a worker. Ordinary DB close may retain bounded warm caches. Initial
packages stay loaded until process exit: `dlclose`/`FreeLibrary` hot unloading is unsupported until
independently proven. Repeated create/destroy is a release gate, not something inferred from OS
process cleanup.

#### NATIVE-407 — configuration and accounting

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativeMemoryEnvelope`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Runtime options MUST accept heap, native allocator, query workspace, input/output staging and worker
limits before engine initialization and expose the effective values through runtime_info before
database use. Persisted per-DB allocator requests cannot silently override the runtime. The default
embedding profile is the explicit table in §3: 256 MiB managed heap, 256 MiB allocator allowance,
two compute workers and 64 KiB cursor chunks. This is not a 512 MiB RSS cap; validation may require
a reviewed specification change before release, never an undocumented runtime substitution. Heap,
explicit native backing, GC-owned native arenas, stack, code and OS page cache are reported
separately. Unsupported/overflow options fail. `global`/skip-close diagnostic arena policies are
forbidden in supported packages.

#### NATIVE-501 — package conformance

- **Level:** MUST
- **Status:** proposed (required for 1.0; implementation pending)
- **Since:** 1.0 / native ABI 1.0
- **Code:** `include/sirix.h`; future `bundles/sirix-native/`
- **Verified by:** `NativePackageMatrix`
- **Evidence class:** doc-only (unverified runtime); header evidence in §5
- **Gap/note:** runtime implementation and conformance suites are follow-up work, not delivered by I1.

Every claimed OS/architecture package MUST pass native execution and all four language flows without
Java, reproduce engine disk-format compatibility and contain the versioned shared binary, public
header, pkg-config/CMake metadata, checksums, licenses/SBOM, provenance and examples. Pin
compiler/toolchain/dependency hashes, including the resolved Brackit snapshot; do not build a
release against an unrecorded moving SNAPSHOT.


## 3. ABI contract details

### 3.1 Representation, options and defaults (NATIVE-401, 404, 407)

[`include/sirix.h`](../include/sirix.h) is hand-maintained public input to C11,
C++17, Rust bindgen and Python cffi. It contains declarations only. Supported
platforms are little-endian, 64-bit pointers, 8-bit bytes, 8-byte alignment for
64-bit integers/pointers, and no packing override. `sirix_status_t` is signed
32-bit; handles are unsigned 64-bit; flags and discriminants are unsigned 32-bit.
Nested structs also have a size/major prefix. Values are passed by pointer, never
by struct value. The header's `SIRIX_*_INIT` aggregate macros work in C and C++.
Initialize output records too; the callee preserves `struct_size` and `abi_major`.

Every v1 record has the size/offset contract in `tests/native/abi-layout.json`.
ABI 1.0 accepts its full v1 size, or a larger size with zero unknown input tail;
a smaller v1 record returns INVALID_ARGUMENT without engine work. Future minors
accept the known v1 prefix and default appended inputs; outputs are written only
to complete fields within advertised size. The embedded instant has a fixed 24-byte
extent for ABI major 1: appending inside it would move enclosing fields and is
therefore forbidden within this major. Other records can grow only at their end.
An unsupported major returns ABI_MISMATCH. Invalid error headers leave error storage untouched; the status is
still returned. Only known-size or zero-tail records are accepted, including
output/error structs; callers zero them before the first call. Known output/error
payload fields are ignored on input, so records can be reused without clearing
the previous result. Unknown input flag bits, reserved nonzero fields and unknown tag values
return INVALID_ARGUMENT; recognized but unavailable future capabilities return
UNSUPPORTED. A present options struct uses its explicit fields, including zero;
only a null options pointer selects the published default (query zero limits
explicitly mean inherit). No unchecked JVM properties or environment overrides.

| Record / field | Null-options default / supported values |
|---|---|
| Runtime heap_bytes | 268435456 (256 MiB), positive |
| Runtime allocator_bytes | 268435456, positive; explicit native backing ceiling |
| Runtime query_workspace_bytes | 67108864 (64 MiB), positive, aggregate runtime query workspace ceiling |
| Runtime max_input_bytes | 16777216 (16 MiB), positive; aggregate payload bytes per call |
| Runtime chunk_bytes | 65536, positive, maximum bytes exported per cursor call |
| Runtime max_depth | 1024, positive serializer/parser nesting ceiling |
| Runtime worker_count | 2, positive compute-worker ceiling, not total process thread count |
| Runtime flags/reserved | 0 |
| Database model/access | JSON / READ_WRITE; JSON or XML, READ_WRITE or READ_ONLY |
| Database flags/reserved | 0; READ_ONLY create returns READ_ONLY |
| Resource backend/versioning | FILE_CHANNEL / SLIDING_SNAPSHOT; these are the v1 creation profile |
| Resource path_summary/node_history | 1 / 1; each accepts 0 or 1 |
| Resource valid_from / valid_to | null + zero length; both set or both absent |
| Resource flags/reserved | 0 |
| Query language | JSONIQ for JSON, XQUERY for XML (null options use transaction model) |
| Query workspace_bytes/chunk_bytes/max_depth | 0 inherits runtime; positive values may reduce, never exceed it |
| Query flags/reserved | 0, always scoped/read-only |
| Selector kind | LATEST for null pointer; REVISION or TIMESTAMP explicitly |
| Selector unused fields | zero (nested instant header initialized); flags/reserved zero |
| Diff flags | 0 = no data; INCLUDE_DATA (1) includes endpoint data |
| Commit message | null + zero means absent; non-null + zero means empty string |

Resource creation persists these explicit settings; other persisted settings use
the 1.0 engine format profile and are reported by `resource_info`. Opening reads
persisted settings, never reapplies creation defaults. Valid-time paths for JSON
use the engine's configured JSON field paths. XML paths are namespace-qualified
child/attribute paths relative to each record element, with EQNames
`Q{uri}local` (empty URI allowed); a final `@Q{uri}local` selects an attribute.
Path components are separated by `/` outside EQName braces; URI slashes inside
braces are part of the name. Empty components, invalid NCNames and braces inside
a URI are rejected. No predicates, wildcards or dynamic expressions are allowed.
The record set is
the document element's element children; absent/unparseable bounds are unbounded.
This XML adapter is required work in the XML parity issue, not existing engine
behavior. Both endpoints are scalar dateTime values parsed as instants.

`runtime_info`, `resource_info` and `cursor_item_info` write one UTF-8 JSON record,
without NUL. Null destination with capacity zero is a size probe. Insufficient
capacity returns BUFFER_TOO_SMALL, sets `needed` to the exact byte count, writes
no destination bytes and does not advance a cursor. Successful calls also set
`needed` to bytes written. Retrying runtime accounting may require a larger
buffer because counters can change; item metadata is stable until cursor advance.

Runtime info schema 1 contains `schema`, `abiMajor`, `abiMinor`, `engineVersion`,
`formatVersion`, `buildId`, `capabilities`, `limits` (the runtime fields above),
and `memory`: `managedHeapBytes`, `allocatorReservedBytes`, `gcNativeBytes`,
`stackBytes`, `codeBytes`, `osPageCacheBytes`. Unsupported measurements are JSON
null, not fabricated zero. The allocator counter is always available. These
categories are separate observations, not a sum advertised as an RSS ceiling.
Resource info schema 1 contains `schema`, `model` ("json"/"xml"), `backend`,
`versioning`, `pathSummary`, `nodeHistory`, `validFromPath`, `validToPath` and
`formatProfile`; absent paths are null. Unexposed format settings are identified
by that versioned profile, not by undocumented current Java builder defaults.

### 3.2 Errors, outputs and state transitions (NATIVE-102–104, 205–206, 402–406)

The complete numeric status manifest is in the header: OK=0, EOF=1, errors
100–129. Codes are never reassigned. `detail_code=0` in ABI 1.0; diagnostic text
is not a query-error identity API. Query errors use SYNTAX for compilation,
QUERY for evaluation, READ_ONLY for forbidden effects. Malformed UTF-8, names,
unknown flags/tags and illegal position use INVALID_ARGUMENT; malformed JSON/XML
uses SYNTAX; model/node-kind mismatch uses TYPE_MISMATCH. Missing node/resource
uses NOT_FOUND; integer/instant overflow uses RANGE. OS I/O errors use IO;
invalid stored data uses CORRUPTION. Semantic failures precede filesystem changes.
When multiple inputs are invalid, no ordering among validation errors is promised.

All non-optional output pointers are required. Output handles, revisions, `needed`
and chunk payload fields start at zero on failure, except `needed` on
BUFFER_TOO_SMALL and a proven committed revision on commit cleanup error. Scalar
outputs are zeroed after validating writable preconditions; an invalid versioned
output record is left untouched. Successful calls and EOF clear valid error
records' payload fields. `err` is always optional; no caller needs it to manage
lifetimes safely. Error messages have `message_len <= 512`, contain valid UTF-8,
are not NUL-terminated, and set TRUNCATED if shortened at a codepoint boundary.
Absolute host paths and full stacks are omitted by default. `revision` is valid
only for COMMITTED. Normally `code` equals the returned status. For TX_FAILED,
COMMITTED_WITH_ERROR, CLOSE_FAILED and RUNTIME_POISONED it contains the first
underlying cause (for example IO or OOM), while the returned status determines
lifetime. Error commit outcomes are NONE=0, NOT_COMMITTED=1,
COMMITTED=2, UNKNOWN=3. The header supplies flags for TX_CLEANUP_ONLY,
RUNTIME_POISONED and HANDLE_CONSUMED; these are diagnostic, not the sole way to
infer mandatory cleanup from a status.

| Operation result | Handle and retry contract, also when err is null |
|---|---|
| Preflight error | No modification; handles remain usable |
| Mutation failure after work starts | TX_FAILED; writer rollback/close-only |
| Commit OK | COMMITTED; revision returned, writer consumed |
| Commit preflight rejection (e.g. BUSY) | No commit attempted; writer unchanged, outcome NOT_COMMITTED |
| Commit fails before publication | TX_FAILED, NOT_COMMITTED; rollback/close-only |
| Commit proven published, cleanup fails | COMMITTED_WITH_ERROR; revision returned, writer consumed |
| Commit publication uncertain | COMMIT_OUTCOME_UNKNOWN; close-only; never automatic retry |
| Rollback OK | Writer consumed, no new revision |
| Rollback cleanup failure after transition | CLOSE_FAILED; writer consumed, no automatic retry |
| Close invalid token / wrong owner / BUSY | No state change |
| Close after terminal cleanup begins | OK or CLOSE_FAILED; token consumed; all independent cleanup attempted |
| Destroy BUSY / preflight error | Runtime live, admission restored, safe retry after fixing precondition |
| Destroy OK | Runtime and sole context consumed; fresh create permitted |
| Destroy terminal failure | RUNTIME_POISONED; runtime/context consumed; no further runtime creation in process |

A poisoned runtime retains any storage still used by undrainable workers until
process exit. It never reports successful teardown or allows hot unload. Ordinary
allocation failure is OOM when state is unchanged; after writer modification it
is TX_FAILED. RUNTIME_POISONED is reserved for terminal destroy failure; native
fatal faults outside that protocol may terminate the host. No status
claims that arbitrary native memory faults can be recovered.

The runtime is not attached by creation. Each OS thread attaches explicitly.
The sole remaining context is needed for destroy. Shareable DB/resource handles
can be closed through any context in their runtime, subject to live-child BUSY.
`handle_close` accepts DB/resource/transaction/query/cursor, not runtime/context;
even zero-handle close requires a valid calling context.
A cancelled cursor remains a live child until explicitly closed; cancel is
idempotent and cross-context within the same runtime. Query rebinding/execution
while its previous cursor lives returns BUSY. A writer with live cursors rejects
mutation, commit and rollback with BUSY; a read cursor pins its snapshot. A query
requires a read transaction (a writer returns HANDLE_TYPE). Mutation payloads are
validated before work; failures that cannot be detected before modification obey
the failed-writer state. Cleanup attempts all actions and counts suppressed errors.

### 3.3 Node operations and typed values (NATIVE-201–207, 301, 401)

Database options declare the expected model on open; mismatch is TYPE_MISMATCH.
Resources inherit that model. Logical resource names are nonempty UTF-8 without
NUL, slash, backslash, `.` or `..` as the whole name. Paths are nonempty UTF-8
without NUL; Windows conversion preserves non-ASCII characters. String/query
values may contain U+0000 where their data model allows it. Every call checks
lengths against SIZE_MAX, engine bounds and the configured input quota before
copying; quota exhaustion is LIMIT, unrepresentable numeric conversion is RANGE.

Node key 0 is the document node; values greater than INT64_MAX return RANGE.
REPLACE_DOCUMENT requires key 0. REPLACE_SUBTREE, FIRST_CHILD, LAST_CHILD,
LEFT_SIBLING, RIGHT_SIBLING and REMOVE use the named node as target/anchor.
Document removal or sibling insertion beside a document is INVALID_ARGUMENT.
`read_json`/`write_json` on XML and `read_xml`/`write_xml` on JSON return
TYPE_MISMATCH. Reads at bootstrap produce an empty sequence. Writer reads see
pending data; other transactions only committed data. Revisions beyond head
return NOT_FOUND, above INT32_MAX return RANGE, retained tombstones return
REVISION_PRUNED. Timestamps use signed seconds plus nanos 0–999999999;
transaction comparisons floor to microseconds, with checked multiply/add even
for negative seconds; valid-time comparisons retain nanoseconds.

`sirix_xml_value_t` describes a node. For DOCUMENT/ELEMENT, `value` is a complete
UTF-8 XML document/one element and name fields are empty. For ATTRIBUTE,
namespace URI/local/prefix identify its expanded name and `value` is unescaped
text. NAMESPACE uses prefix plus URI, with empty local/value. TEXT/COMMENT use
only value; PROCESSING_INSTRUCTION uses local as target and value as content.
For non-container kinds, child insertion attaches to the specified element;
attributes/namespaces cannot have siblings in the child axis. REPLACE_SUBTREE
on a non-structural node replaces that node, with namespace consistency checks.
XML parsing disables external entities/DTD resolution and preserves the Sirix
XML node model, not original lexical spelling, quote style or CDATA boundaries.

`sirix_value_t` has a tag and explicit scalar/pointer fields, not an ABI union.
Only the selected field is nonzero (nested headers remain initialized): JSON and
STRING and DECIMAL use data/len; BOOLEAN uses boolean_value (0 or 1); INT64 uses
integer_value; INSTANT uses instant; XML uses the required `xml` pointer to an
XML value. JSON null is JSON text `null`, not a null pointer or empty sequence.
DECIMAL is the XSD decimal lexical grammar with no exponent; invalid lexical
values return INVALID_ARGUMENT. Variable local names are NCNames; URI/local is
an expanded name and prefix resolution never rewrites query text. All payloads,
including nested XML fields, are copied at bind time. Bindings remain for later
executions until replaced or the plan closes. Input pointers are never retained.

### 3.4 Framing and schemas (NATIVE-301–305, 401)

`cursor_read` emits at most one item fragment. Capacity zero returns
BUFFER_TOO_SMALL without advancing (including at EOF); positive capacity is
capped by configured chunk_bytes. Each item starts with BEGIN=1, ends with END=2,
and has a zero-based item_index constant across fragments. A single fragment
sets both bits. Item kind is stable across fragments. An empty lexical item emits
zero bytes with BEGIN|END and OK; empty sequence returns EOF with zeroed chunk
payload. Repeated EOF is EOF. CANCELLED remains CANCELLED until close. Other
iteration errors are sticky until close, with no further bytes. Metadata access
before the first successful read or after EOF returns NOT_FOUND. Metadata for
an ended item remains available until the next successful read. A too-small
metadata buffer never consumes data. Any failed read clears chunk payload fields;
previous chunks form only a prefix, never an implicitly complete failed item.

| Item kind | Payload and metadata |
|---|---|
| JSON=1 | One JSON value, UTF-8, preserving numeric lexical precision |
| XML=2 | DOCUMENT/ELEMENT XML serialization with namespace fixup and no XML declaration; other node kinds carry their unescaped string value |
| ATOMIC=3 | XSD lexical value, type URI/local in metadata; strings unquoted; booleans `true`/`false`; decimal no exponent; integer decimal; float/double XSD scientific/INF/-INF/NaN; date/time timezone retained; QName prefix-independent `Q{uri}local`; binary base64 or hex according to type |
| JSON_DIFF=4 | One existing JSON semantic-diff document (not RFC 6902) |
| XML_DIFF=5 | One JSON event per full XML diff callback, schema below |
| HISTORY=6 | One JSON revision metadata object, schema below |

XQuery unsupported function/map-like values without a defined lexical encoding
return TYPE_MISMATCH, not Java `toString()`. XML attribute/namespace/PI names are
recoverable from metadata, so their string payload is never mistaken for JSON
or an element. Adjacent XML text nodes remain distinct items. UTF-8 may split
between chunks: decode after assembly or incrementally, not each chunk separately.

Item metadata schema 1 has `schema`, `itemIndex`, `kind`, `typeUri`, `typeLocal`,
`nodeKind`, `nameUri`, `nameLocal`, `namePrefix`, `nodeKey`, `revision`. Kind uses
the numeric codes above; nodeKind uses header XML node codes or 0 when inapplicable.
Absent fields are JSON null (nodeKind stays 0). Stored nodes carry key/revision;
constructed nodes have null key/revision. Atomics carry their expanded XSD type,
JSON items use null type names. XML node names are null for unnamed kinds.
A node stream from a writer uses null revision until committed. All integer
metadata remains exact; bindings do not round uint64 values through doubles.

XML diff uses `DiffFactory` full XML events, including attributes/namespaces,
with hashing disabled for a deterministic event oracle. Each event is
`{"schema":1,"op":"UPDATED","oldNodeKey":7,"newNodeKey":7,
"oldDepth":2,"newDepth":2,"oldData":null,"newData":null}`.
`op` is one of SAME, SAMEHASH, INSERTED, DELETED, UPDATED, REPLACED,
REPLACEDNEW, REPLACEDOLD, MOVEDFROM, MOVEDTO; emit the callback order without
inventing move pairing. Keys/depths preserve the Java event (a counterpart can
be an anchor). Without INCLUDE_DATA both data fields are null. With it,
`oldData`/`newData` describe actual endpoint nodes at those keys as objects with
`nodeKind`, `nameUri`, `nameLocal`, `namePrefix`, `value`; value follows XML item
encoding. Absent endpoint data is null. This is an event stream, not a promised
standalone patch language. Independent changed-node fixtures complement the Java
oracle; JSON additionally uses replay fixtures. Equal/reversed diff endpoints
return INVALID_ARGUMENT even if documents at distinct endpoints are identical.

History captures H at open, takes explicit inclusive first/last revisions
(1 <= first <= last <= H), and emits newest-first records:
`{"schema":1,"revision":2,"timestamp":{"seconds":0,"nanos":0},
"author":null,"message":null,"retained":true}`.
Bootstrap 0 is excluded; an invalid range returns INVALID_ARGUMENT and a range
beyond H returns NOT_FOUND. Null differs from an empty author/message. Pruned
records remain present with retained=false and available timestamp; unavailable
optional metadata is null. Internal page size is bounded by workspace quota;
page boundaries do not change records, ordering, or the captured head.

## 4. Requirement-to-suite traceability and derived tests

All named runtime suites below are **planned**, not existing tests. Each row
covers the complete requirement, including every MUST and its §3 refinements.
Implementations add failing tests before wrappers and tag tests/enforcement sites
with the requirement ID. JSON and XML run the same lifetime/error/buffer matrix;
model-specific fixtures add XML namespaces, attributes, mixed text, comments,
processing instructions, constructed nodes and typed XQuery atomics.

| ID | Named suite(s) | Required cases / independent oracle / mutations to kill |
|---|---|---|
| NATIVE-101 | NativeNoJvmConformance | Clean OS C/C++/Rust/Python persist/reopen/query/diff/history for both models; loader and process audit reject Java/REST fallback |
| NATIVE-102 | NativeRuntimeLifecycle | Second create, no implicit context, limits before initialization, stale epoch after restart; remove singleton guard |
| NATIVE-103 | NativeThreadOwnership | Wrong thread, duplicate attach, detach BUSY, cross-context cancel, async owner pinning; remove ownership guard |
| NATIVE-104 | NativeHandleStateMachine | Seeded valid-storage handle walks, stale/wrong-kind/wrong-runtime tokens, generation exhaustion; remove generation/type checks |
| NATIVE-201 | NativeDatabaseContract | Create-existing never overwrites, open-missing never creates, model mismatch, Unicode/aliases, invalid UTF-8/NUL, subprocess locks; invert create/open checks |
| NATIVE-202 | NativeRevisionOracle | Bootstrap, head capture under commit, int overflow, tombstone; remove head pin/prune check |
| NATIVE-203 | NativeRevisionOracle | Linear timestamp oracle, commits one microsecond apart, adjusted ties/regressions under TXN-310, negative epoch, before/after head, overflow; flip <= or drop ceiling |
| NATIVE-204 | NativeWriterAdmission | Latch-controlled same-resource races across aliases, independent resources, reads during write; remove core admission/release |
| NATIVE-205 | NativeTransactionModel | Model replay of explicit actions, malformed JSON/XML, XML attr/ns/name constraints, uncommitted isolation, cursor BUSY, commit consumption; enable auto-commit or omit rollback |
| NATIVE-206 | NativeCommitFaults | Fault before/after publication and each cleanup action, null error storage, kill/reopen recovery; swap outcome/consumed status or omit independent cleanup |
| NATIVE-207 | NativeBitemporalDifferential | Scan/index/simple interval oracle for both models, nanosecond boundaries, absent/invalid bounds, missing config, pruned timestamp; flip half-open predicate |
| NATIVE-301 | NativeQueryDifferential, NativeReadOnlyPolicy | Java Brackit corpus, typed copied bindings, XML mixed/constructed nodes, namespaced variables, lazy/worker mutation rejection and disabled I/O; remove copy/policy |
| NATIVE-302 | NativeCursorFraming | Empty sequence vs null vs empty string/text, 1-byte/64 KiB buffers, astral Unicode, precision, lexical atomics, metadata lifetime, error prefix; alter flags/count or advance metadata probe |
| NATIVE-303 | NativeCursorLifetimeBudget | Oversized item, sorting/diff quota, early close, cancel with worker active, allocation/pin/worker counters after warm-up; remove bound, allocate per row, free before fence |
| NATIVE-304 | NativeDiffDifferential | Java JSON/full XML diff plus JSON replay and XML changed-node fixtures; attrs/ns/text/PI, all operation vocabulary, include-data, missing/pruned/reversed endpoints; drop event or endpoint check |
| NATIVE-305 | NativeHistoryPaging | Range partition property, concurrent commit, retained/pruned revisions, absent vs empty metadata, bounded pages/pins for both models; skip tombstone or recapture head |
| NATIVE-401 | NativeBufferContract | Valid pointer storage with null/zero, overflow, input/output canaries, copied inputs, optional outputs, no partial metadata write; off-by-one/bypass cap; ASan/UBSan |
| NATIVE-402 | NativeHandleStateMachine | Every parent/child close permutation, repeated close, zero close, shared-session refcounts; cascade prematurely or fail to consume |
| NATIVE-403 | NativeErrorMapping | Fixed code mapping, low-memory reporting, short error struct, UTF-8 truncation, null err with terminal status; uncaught exception/status mutation |
| NATIVE-404 | NativeAbiCompatibility; NativeHeaderContract (delivered) | Layout/width/alignment, C11/C++17, cffi/bindgen, unknown flags/major/size, old client/new library per platform; reorder/widen field or break extern C |
| NATIVE-405 | NativeAbiCompatibility; NativeHeaderContract (delivered) | Pre-create version/caps, all 255 bits, ABI-major loader names and stable symbols; missing bit/symbol/version mutation |
| NATIVE-406 | NativeRuntimeLifecycle | Repeated cycles, live-child BUSY, worker drain failure retains storage, terminal poison prevents recreate, no unload; reverse drain/free or omit poison |
| NATIVE-407 | NativeMemoryEnvelope; NativeHeaderContract (default constants only) | Effective limits, no persisted override, quota/admission exhaustion, accounting baseline, overflow/options; remove charge/refund or use global arena |
| NATIVE-501 | NativePackageMatrix | Native execution on each claimed platform, all four languages, frozen format round-trip with JVM oracle, reproducible pins/SBOM/metadata/examples; remove package component |

Use fixed logged seeds for release regressions and explore generated JSON/XML,
UTF-8 chunk partitions, handle operation sequences and revision histories. Do not
fuzz invalid non-null addresses in-process. Fault/crash cases run in subprocesses.
PIT covers Java facade logic; scripted C mutations/native mutation tools cover
the facade and header contracts. Resolve survivors under
[CONV-103](CONV-conventions.md#conv-103--specification-before-implementation). No Java library
logic changes in I1, so PIT and engine work budgets are not I1 evidence.

Read/load/query changes in later issues run the complete **Work budgets** block
of [VERIFICATION](../docs/VERIFICATION.md#running-the-layers), plus bridge allocation
checks. Never widen bounds or add wall-clock correctness assertions. Benchmarks
run separately. Implementation bugs get the repository postmortem template,
a regression and a lesson as required by [CONTRIBUTING](../CONTRIBUTING.md).

## 5. Delivered acceptance checks and remaining questions

`python3 tests/native/check_header.py --output-dir /tmp/sirix-header-check`
compiles/runs C11 and C++17 clients, validates every struct field offset/size and
alignment against the explicit ABI manifest, generates/compiles/runs Rust bindgen
layout tests, and parses preprocessed declarations with cffi, checking its layouts
against the same manifest. It checks all numeric constants, default aggregates and
the complete exported function type surface. It never links or pretends to test
an implementation. `--mutations` checks deliberate contract violations in scratch
copies. See [tests/native/README.md](../tests/native/README.md) for prerequisites
and the CI command. This is NativeHeaderContract only; no runtime suite is green
because a header compiles. No existing native executable or Gradle target changes.

I2 adds a separate [core/query C shared-image smoke](../tests/native/SMOKE.md),
with core-owned reachability metadata and an explicit snapshot-worker shutdown
hook. Its private test entrypoints are not the public ABI. The smoke checks a
small JSON persist/reopen/query flow, an empty runtime environment, loader and
exec traces, and isolate teardown. These are partial regression checks for
NATIVE-101/102/201/205/301/406, not completion of their full conformance suites.
C++/Rust/Python, XML parity, clean package installation and repeated runtime
ownership remain required release work.

Open engineering questions (not optional release scope):

- I4: integrate the snapshot append/serialization terminal hook after writer
  admission stops, then retire caches and allocator reservations and prove
  repeated isolate cycles. I2 identified custom snapshot append workers as the
  teardown laggards ([postmortem](../docs/postmortems/native-snapshot-executor-teardown.md));
  the complete runtime owner and repeated teardown remain release gates.
- I6/XML parity: implement the XML valid-time path adapter above and choose the
  index implementation; scan/index equivalence is required before release.
- I8/XML parity: prove the full XML diff observer and include-data serializer
  remain within admitted workspace for deep/wide documents; the current list
  implementations are not evidence of bounded delivery.
- I9/I11: certify platform OS/deployment floors, AOT toolchain versions and real
  default memory envelopes on native runners. Do not claim untested platforms.
- CONF/FMT chapter integration: reconcile the explicit native creation defaults
  and formatProfile identity when those chapters land; changes require spec and
  fixture review, never silent adoption of mutable Java defaults.

No unresolved item permits shipping a partial stable JSON-only library, calling
ABI 0 stable, omitting a language binding or moving functionality to 1.x.

Open capability, not required for 1.0:

- A native conditional-write entry point and SDK exposure are planned future capabilities.
  When added, they will reuse the Java core check of JAPI-111 and the semantics of TXN-311.
