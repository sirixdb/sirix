# SirixDB Strategy

## Vision

<!-- vision: author-approved 2026-11-22 -->

`SirixDB` exists so that the past of your data is as queryable, and as fast to query, as its present.
It serves engineers, and the agents that work for them, who must answer what a record said at an earlier moment, when that was true in the world, and how the data changed in between, without building a second system to keep history.
It turns every commit into an immutable revision, addressed by transaction time and by valid time, and keeps each one readable.
It owns exactly one thing: the bitemporal storage engine, and the contract of its bytes, its results and its cost.

### The advantages to protect

Versioning reaches below the page: a commit does not copy the data pages it touches, it writes a fragment for each.
Under the default sliding-snapshot strategy a fragment holds the records that changed and the few that must be carried forward so that reads stay bounded.
Above the fragments a commit rewrites the copy-on-write path up to the new revision root, and nothing else.
Any page of any revision is rebuilt from a bounded number of fragments.
So storage grows with what changed and with the height of that path, never with the size of the data, and a read of the past is direct page access whose cost does not grow with the length of the history behind it.
Identity reaches below the document: every object, array, field and value keeps one key across all revisions, so the history of a single node and the node-level difference between two revisions are direct questions.
Both time axes live in the engine and not in the application: the system keeps transaction time, valid time is indexed, and one query can fix both.
Every index and every derived structure is versioned with the data: opened at revision N it answers as of N, so analytics over a past revision take the same path as analytics over the present.
All of this is available inside the application's own process, with no server to run.
These are what the engine offers that a history table, an event log or a snapshot store does not, and a change that weakens one of them to gain what other engines already have is resisted.

### History is the data model

No commit overwrites data.
A committed revision stays readable until retention or erasure that the user ordered removes it, and what was removed is marked as removed, never left as a hole or a silent wrong answer.
The history of a resource is a single line of revisions.
Branching is resisted for as long as a fork would let two nodes share an identity: it needs an identity scheme that survives a fork first.
A derived structure may be maintained after the commit, but a query then waits for it or falls back, and never answers from a stale state.
Reverting appends a new revision and removes none.
A change that makes the past second-class - slower to reach, approximate, or stored apart from the present - is refused.

### Speed is part of correctness

An engine that quietly does more work per row is as broken as one that returns the wrong row.
A change to a read, load, commit or query path is accepted only when it makes no measured path slower, cold paths included.
A change that buys a smaller store or less memory at the price of latency ships only as a persisted setting that is off by default, so that the default path stays the fastest one.
Analytical speed is in scope, within one revision and across many.
Aggregates, group-bys and joins over any one revision, and questions about how data changed over a span of revisions or of valid time, are meant to be fast, and run under the same budgets and the same fail-closed rule as every other path.
Cost is asserted as counted work - pages read, routes taken, bytes allocated - and never as a wall-clock threshold.
A bound is never widened to get a green build.
A timed claim follows a plan written before the run, is measured in alternating fresh-process pairs, and is published with its method and with the cases it loses.
Work done per record, node, page, key or row allocates nothing per element.
Inside the engine, when speed and convenience conflict, speed wins.

### A fast answer is the same answer

Every fast path is fail-closed: it serves a query only when it can prove its result equals the general path's, and otherwise it declines.
An index, a projection or a cache changes what a query costs and never what it returns.
A wrong or partial answer is a defect to fix, never a limitation to document.
A structure that cannot be proved valid is refused before it is published, because a page written wrong today is still read years later.

### The specification decides behaviour

Behaviour is written as a numbered requirement before it is implemented, and a change that needs unspecified behaviour starts by specifying it.
Tests are derived from requirements and assert behaviour only: results, errors, persisted bytes and cost.
A requirement reports its own evidence honestly, and says gap where a test is missing.
Changed logic is mutation-tested, and no test is ever written to kill a mutant or to cover a line.
A bug fix is a root-cause write-up, a regression test that failed first, and a lesson.
A change is reviewed against its written intent, and only the owner of that intent widens it.

### One format, one route

1.0 has exactly one on-disk format, and the engine contains no code for reading anything older.
A pre-1.0 store is refused with a clear error and migrates by exporting and re-importing its history.
From 1.0 on, every release reads every store that a released version wrote: when an encoding is replaced the reader keeps the old one, as other databases do, and a frozen corpus of stores that every later reader must pass holds it to that.
A new encoding is gated for one release, the way MongoDB, CockroachDB, Cassandra, RocksDB and DuckDB gate theirs: the release that introduces it reads it, and a resource writes it only after its owner switches it on, a persisted switch that cannot be undone.
The release after that switches every resource on its first write and deletes the old writer, so an operator can go back for one release and no old writer lives for good.
Readers are strict: an unknown required feature, a nonzero reserved byte or an out-of-range id is refused, never guessed at.
The format grows only through the extension points it reserved.
A persisted, specified setting of a resource - its versioning strategy, an opt-in layout - is part of the one format and not a second one: a reopened resource obeys what was persisted, and every setting returns the same results.
Within those settings each operation has one route: no silent fallback to another backend, and no second implementation kept for old callers.
The one exception is announced: a stable API is replaced only after a deprecation notice, stays for the announced window, and is then removed.

### A library first

The engine is an embeddable library: one process, one directory, no sidecar and no cluster to operate.
One process owns a database and one writer at a time writes a resource, so readers are never blocked and writes never conflict.
The REST server, the CLI, the MCP server and the native C binding are layers over the same engine: they expose its contract and add no second implementation of it.
JSON is the first-class data model.
XML is kept correct and follows: a capability may ship for JSON first, and XML is brought up after it, not left behind.
Queries are written in JSONiq and XQuery, the languages that fit versioned trees, and more and more of those queries are written by agents.
A SQL front end would be welcome, but it belongs in the query compiler or in another query engine, reached through the query bundle or a bundle of its own, never inside the storage engine, and it never comes before the engine's own work.
Agents are users in their own right: through the agent surface an agent explores, queries, analyses and edits the data on its own, with no person writing the query for it.
That surface is read-only by default, and every edit an agent makes is a revision that can be inspected, diffed and reverted.

### Scope

This repository is a single-node engine and not a distributed database: replication, sharding and consensus are outside it.
A distributed database can be built around this core, as the enterprise repository may do and as anyone else who wants one is free to do.
A storage backend built for absolute speed belongs to that enterprise layer as well.
SirixDB stores versioned JSON and XML trees, not relational tables.
It is not a home for adapters to other projects' frameworks: those are built outside this repository, on its public surfaces.
It does not claim what it cannot show: a comparison whose method is unsound is withdrawn, and known limitations are listed, not hidden.
This repository holds itself to the same standard, so its documents state what is specified, what is implemented and what is only planned.

A change aligns when it keeps every revision readable and every result identical, makes no measured path slower, cites the requirement it implements, keeps every released store readable, and leaves one route behind it.
A change should be resisted when it trades a correct answer for a fast one, makes the past cost more than the present, reads a pre-1.0 store or stops reading a released one, adds a second route for one operation, widens a bound or weakens a test to pass, or weakens one of the engine's own advantages to gain what other engines already have.
