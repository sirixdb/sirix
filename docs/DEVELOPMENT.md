# How SirixDB is developed

SirixDB never overwrites. Every revision it has ever committed stays readable, which makes a
correctness bug unusually expensive: a page written wrong today is still being read in five years,
and the first symptom can be a query against an old revision long after the commit that caused it.
The same property makes speed a contract rather than a preference — an engine that quietly starts
doing more work per row is as broken as one that returns the wrong row, and nobody notices until a
database is large.

This document is the project's development strategy: how we decide what to build, how tests come to
exist, how we know a change is finished, and why each of those practices is the way it is. It is
written for contributors and for the AI agents that do much of the work here. Both need the reasons
and not only the rules, because a rule whose reason is unknown gets satisfied literally and defeated
in substance. TigerBeetle's [TIGER_STYLE.md](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/TIGER_STYLE.md)
and SQLite's [How SQLite Is Tested](https://www.sqlite.org/testing.html) are the models for its
form: one public narrative instead of folklore spread over review comments.

It explains the *why*. The authoritative detail lives in the documents it points to:

| Document | What it owns |
|---|---|
| [spec/README.md](../spec/README.md) | The requirement chapters: format, transactions, versioning, configuration, APIs. |
| [spec/CONV-conventions.md](../spec/CONV-conventions.md) | Requirement conventions, IDs, evidence rules, mutation policy (CONV-201 to CONV-206) and the official work budgets. |
| [docs/VERIFICATION.md](VERIFICATION.md) | Every verification layer, the bug class it catches, how to run it, and the PR mutation lane. |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | Build, branch and pull-request mechanics. |
| [AGENTS.md](../AGENTS.md) | The HFT and low-latency hot-path rules every change must respect. |
| [budget README](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md) | Work-budget rules and how to add one. |
| [docs/postmortems/TEMPLATE.md](postmortems/TEMPLATE.md), [docs/LESSONS.md](LESSONS.md) | Bug write-ups and the lessons taken from them. |
| [docs/BENCHMARK_CAMPAIGNS.md](BENCHMARK_CAMPAIGNS.md) | The performance campaign record, its numbers and its measurement discipline. |

## 1. The specification is the source of truth

Behaviour is decided in `spec/` before it is implemented. Each chapter states numbered requirements
— `FMT-204`, `TXN-311`, `VER-201` — and every one carries its level (MUST, SHOULD), its status, the
code that implements it, the tests that verify it, an evidence class and a note naming what is still
missing.

The IDs are the point. A requirement ID is something an issue, a test, a review comment and a
changelog entry can all cite, and it still means the same thing a year later; a paragraph of prose in
a closed pull request does not. So work flows in one direction: **the spec first, then an issue that
names the requirement IDs it is about, then a pull request that cites them.** When the spec is silent
on the behaviour a change needs, the change begins by filling that gap rather than by guessing, and
the guess becoming permanent is exactly what the order prevents.

The second rule of the spec is honesty about its own evidence
([CONV-102](../spec/CONV-conventions.md#conv-102--conformance-and-evidence-honesty)). A requirement
is `enforced` only when the behaviour exists and a mapped deterministic test passed in a default CI
lane; otherwise it says `partial`, `gap`, `designed`, `proposed` or `open`, and the gap note says
what is absent. A specification that overstates its own evidence is worse than no specification,
because it moves the reader's attention away from the one part that needs it.

## 2. Tests are derived from the requirements, not from the code

A test written after the implementation, by reading the implementation, re-states what the code
does. It passes, it survives mutation, and it proves nothing about whether the behaviour is right —
it has recorded the bug along with everything else. So:

- **Feature tests are derived from the spec, before the implementation or independently of it.**
  Writing the oracle from the requirement forces the requirement to be precise enough to test, which
  is most of the value of writing it down at all.
- **A bug fix carries a regression test that fails on `main` first.** The failing run is the only
  evidence that the test actually observes the bug. A test added beside a fix and never seen to fail
  may be checking something adjacent to the defect, and will keep passing the next time the defect
  returns.
- **Implementers never weaken a spec test.** If an implementation disagrees with a test derived from
  a requirement, either the implementation is wrong or the requirement is wrong. Changing the test's
  oracle to match the code turns a caught failure into a silent behaviour change. Change the
  requirement instead, in its own change, with the reason recorded
  ([CONV-105](../spec/CONV-conventions.md#conv-105--compatibility-and-versioning)).
- **Tests assert behaviour only.** No reading private state, no asserting on internal call sequences,
  no tests aimed at a particular line. The engine's internals move constantly — the HOT index alone
  has been through several encodings — and a test bound to internals is a tax paid again at every
  refactor while saying nothing about the contract. The deliberate exception is cost: a work budget
  asserts on counters the engine maintains, because cost *is* part of the observable contract
  (§6), not because the counters are convenient to reach.

### Independence without mandatory separate authorship

SQLite's test suite is written against the documented requirements rather than against the
implementation, with evidence marks tying each requirement to the tests that check it. We take that
model: **every requirement ID maps to the tests that verify it, and every test cites the requirement
it checks.** Tracing both directions is what makes the suite auditable — an unmapped MUST is visible,
and a test that cites nothing is visible too.

We do not require that a different person or agent write the tests. Independence here is bought by
three other things: the bidirectional tracing above, review against the spec rather than against
the code (§8), and mutation testing (§4), which measures whether the tests assert anything at all,
regardless of who wrote them. For the maintainers' agent-built features one more layer is added on
top: a separate test author who works from the specification without seeing the implementation.

## 3. Requirements and acceptance criteria worth writing

A requirement states the behaviour, the invariants that must still hold after it, the errors it must
raise and its cost. It does not state how the code achieves any of that; a requirement that
describes an implementation cannot survive the implementation being replaced, and it silently
forbids better ones.

Acceptance criteria must be observable through an interface the project supports — a result, an
error, persisted bytes, a counter, a documented API. "Works correctly" is not acceptance; "a reader
opened at revision 3 returns the pre-move subtree, and the move is visible at revision 4" is.

For a documentation or CI change, name the observable acceptance checks — the links resolve, the
generated file matches, the validator rejects this fixture — instead of inventing application tests.
A unit test invented for prose asserts only that the prose exists, and it has to be maintained
forever.

## 4. Mutation testing, as a complement

A suite can execute every line of a change and assert almost nothing. This is the characteristic
failure of machine-written tests, and of human tests written against the code: they call the method,
then check something that was going to be true either way. Mutation testing is the one layer that
measures this directly. PIT changes the production code — flips a comparison, drops a call, returns
a constant — and reports which changes no test noticed. Coverage says a line ran; a killed mutant
says a test disagreed with a wrong version of it.

The pull-request lane judges a PR by **its own changed lines**, not by the classes it touched. A
whole-class criterion leaves every large change red for test debt it did not introduce, which
teaches authors to shrink diffs rather than to test them; the lane's exact attribution and
completeness rules are in
[VERIFICATION.md](VERIFICATION.md#blocking-criterion-the-prs-own-lines).

Every surviving or uncovered changed-line mutant is resolved in exactly one of four ways
([CONV-103](../spec/CONV-conventions.md#conv-103--specification-before-implementation)):

1. **A behaviour test, derived from a requirement, detects it.** The normal outcome. The mutant has
   pointed at a requirement whose test was too weak, so the fix is a better oracle for that
   requirement — not a test aimed at the mutant. Sometimes such a test already exists and the lane
   simply did not select it; then the fix is an association that makes the lane run it, never a
   record claiming the mutant is harmless.
2. **It is recorded as equivalent, with a reason.** The argument must come from the specification,
   the public contract or a documented invariant, and must show that no input the contract admits
   can tell the original from the mutant. Observable behaviour includes results, errors, persisted
   state and cost, so a mutant that only changes cost is not equivalent: it belongs to an
   allocation, work-budget or latency test — in the lane where that test can run, or under (3) when
   it cannot — and a cost that no test bounds needs a budget of its own. One narrow exception to the
   cost rule is accepted: a mutant that produces the same result and differs only at a single
   boundary input, by at most one extra constant-time step.
3. **It is recorded as killed out of lane, with counterfactual evidence.** Some effects are only
   checked by tests that cannot run under mutation instrumentation, such as a clean-JVM allocation
   budget. The record names that test and carries the evidence that it fails when the mutant is
   injected and passes without it: the injection patch, the command, both exit statuses, the failing
   output and the hashes of the touched sources.
4. **The mutated code is removed.** A mutant with no observable effect at all is telling you the
   code has none either.

An *uncovered* changed-line mutant is never recorded as either kind: a mutant that no test executed
has shown nothing about the code, so only a behaviour test, a lane association that makes existing
tests run, or removing the code resolves it. Untested new code is a finding, not the absence of one.

Both kinds of record are per-mutant, reviewed beside the diff and re-confirmed when their method
changes. There are no class-, file-, line- or mutator-wide waivers, because a waiver that broad
removes the signal silently and permanently. The full rules — the record format, the verdicts, the
review obligation and the local helpers — are CONV-201 to CONV-204 in
[spec/CONV-conventions.md](../spec/CONV-conventions.md), with the workflow in
[VERIFICATION.md](VERIFICATION.md#pr-mutation-testing).

**A test is never written to kill a mutant or to cover a line.** Such a test is necessarily bound to
the implementation that produced the mutant, which is what §2 forbids, and it makes the suite worse
while making the report green. A surviving mutant is a question about the specification; answer that
question.

## 5. Coverage is a diagnostic, not a target

An uncovered branch in changed code is treated exactly like a surviving mutant: either some
specified behaviour needs this code and gets a requirement-derived test, which then happens to cover
it, or the code has no specified behaviour and is removed. There is no coverage percentage to hit,
and a test is never written to light up lines.

SQLite reaches 100% branch and MC/DC coverage, and it gets there through requirement-derived tests
in a dedicated harness over a feature set that barely changes. We take that principle and not the
number: a coverage percentage used as a gate in a fast-moving engine buys line-shaped tests — the
implementation-bound kind §2 rejects — and the number then certifies them. (One lane-level floor
does exist, on the small Python CI helper scripts; that is CI plumbing, not a target for product
code.)

## 6. Performance is part of correctness

Two layers guard it, and they catch different things.

**Work budgets** assert on counters — leaves read, routes taken, pages pinned, allocations — around
one operation, and they assert no wall-clock time: a time threshold on a shared runner is flaky and
cannot say what changed, while a counter is exact on any machine and names the path that grew. A
broken budget is a regression until shown otherwise. A bound is never widened to get a green build,
and a wall-clock assertion never replaces one
([CONV-301](../spec/CONV-conventions.md#conv-301--official-work-budgets), and the
[budget README](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md) for how to add one).
The hot-path rules these budgets defend — no per-element allocation, primitive collections,
monomorphic calls, cache-friendly layouts — are in [AGENTS.md](../AGENTS.md).

A green budget suite does not mean nothing got slower. It sees a change in the amount of counted
work and is blind to the same work done more slowly, which is how two benchmark queries once lost
about 150 ms each with every counter unchanged. That is the second layer's job.

### The benchmark protocol

Timed measurement is published in full, because a performance claim without its method is not a
claim. ClickHouse's ClickBench and DuckDB's published comparisons set that expectation for this
class of system; [BENCHMARK_CAMPAIGNS.md](BENCHMARK_CAMPAIGNS.md) is our record, including the
levers that were measured and rejected.

- **One timed campaign at a time, on a quiet machine.** Two campaigns on one box measure each other.
- **Alternating fresh-process pairs**: old, new, old, new, each arm in its own process. Block
  measurement on a thermally throttling machine once faked a 1.7× regression, and within one process
  JIT and page-cache state carry from the first arm into the second.
- **The plan is written down before the run**: what is compared, how many pairs, and the decision
  rule. A bar chosen after seeing the numbers is not a bar, and extending a run until its interval
  excludes zero invalidates the interval.
- **The bar for accepting a change on a timed path**: the paired point estimate at most +1%, the
  lower 95% bound at most 0, the median allocation delta at most 0 rounded to whole bytes, and cold
  paths included in the comparison — a change that is free when warm and expensive on a fresh
  process has not been measured until the cold arm is in the pairs.
- **A disturbed run is rejected on evidence of outside load, never on its result.** Load from
  another process or a competing job justifies discarding a run and retrying it; a number that
  merely looks wrong does not, and neither does throttling that the measured workload caused
  itself. Re-running until the answer is agreeable is how a campaign fools itself.
- **A failed gate is rerun whole**, never only its failing benchmarks, and never with more pairs
  added after a failing result. Both shortcuts turn a fixed-sample decision into a search for a pass.

Machine-specific details — core assignment, load limits, lock paths — belong to the harness
configuration, not to the protocol. The campaign document's
[measurement discipline](BENCHMARK_CAMPAIGNS.md#3-measurement-discipline-why-the-numbers-can-be-trusted)
holds the rest of the hard-won rules, such as proving a route by its counter rather than by its
timing.

## 7. Exploration beyond the happy path

Requirement-derived tests check the cases someone thought of. The bugs that reach a storage engine
are in the cases nobody did, so every change that touches load-bearing logic is also explored:
adversarial inputs, property-based tests that generate and shrink their own counterexamples, and
differential comparison against an independently written oracle. The model-based oracle found a real
bug on its first run; the layer map in [VERIFICATION.md](VERIFICATION.md#layer-map) lists what each
of these harnesses proves and how to run it.

Record the cases, seeds and outcomes in the pull request, and promote every discovered failure to a
deterministic regression with its seed pinned. A random failure that is not pinned is a bug report
you cannot re-run.

## 8. Review against the written intent

Every change carries a written intent: what it must achieve, in the words of whoever asked for it.
**That intent is the acceptance contract.** Review asks whether the change does what the intent says
— not whether the code looks reasonable, which is a much weaker question. **Only the intent's owner
widens it.** A reviewer who thinks the intent is too narrow says so and asks; an implementer who
discovers that more work is needed records it and does not quietly absorb it.

The reason is that the dominant failure mode in both human and agent work is not bad code. It is
delivering something else: a scope narrowed because one part turned out to be hard, an unrequested
extra folded in, a goal reinterpreted into an easier one. Ordinary code review cannot catch any of
that, because the delivered code is self-consistent and often good. Only a comparison against the
written intent catches it, which is why the intent is written down before the work starts and is
widened only by whoever owns it.

The maintainers run this with `no-mistakes`, a pipeline that takes the intent as its input and runs
review, tests, lint, documentation checks, the mutation lane and CI against it, holding the change
at each gate until the finding is answered. **Outside contributors are not required to install it.**
Open a pull request with the intent stated plainly in the description — what it must achieve, and
which requirement IDs it is about — and a maintainer applies the same intent review. PostgreSQL's
commitfest process is the precedent: a named reviewer reads a submission against what it claims to
do, in rounds, before any committer takes it.

## 9. Every bug leaves a lesson

A bug fix is three artifacts, not one: a root-cause [write-up](postmortems/TEMPLATE.md), a
regression test that fails before the fix, and an entry in [LESSONS.md](LESSONS.md).

The write-up's load-bearing field is *why tests missed it*. The root cause explains this bug; that
field explains the hole that let it through, and it is the only part that prevents the next one.
Name the broken invariant and the causal chain rather than the failing line, say which layer should
have caught it, and state what changes so the next bug of that shape is caught — then make the
lesson concrete enough to apply, and extend an existing lesson when a failure mode recurs rather
than adding a near-duplicate.

## 10. One format, no compatibility code

SirixDB 1.0 has exactly one on-disk format, and the engine contains no code for reading any other.

Compatibility code in a reader is permanently untested surface: it runs only for inputs no current
test writes, so it rots quietly and fails on the one store that actually needs it. The temporal
design makes this sharper than usual. Because old pages are never rewritten, every format variant
the reader admits stays alive forever in somebody's history, and each one multiplies the states
every later change has to be correct against. One format is far cheaper to verify than two formats
and a conversion in the read path.

## The short version

1. Write or cite the requirement first. Open an issue that names its ID; cite the ID in the PR.
2. Derive the tests from the requirement, before or independently of the implementation. For a bug,
   watch the regression test fail on `main` first.
3. Assert behaviour only. No private state, no internal call counts, no line-targeted tests.
4. Explore beyond the happy path, and pin every discovered failure with its seed.
5. Mutation-test the changed logic. Kill each survivor with a behaviour test, record a reviewed
   disposition with its reason and evidence, or delete the code. Never write a test to kill a mutant.
6. Treat coverage as a diagnostic. Explain an uncovered branch; do not chase a number.
7. If it touches a read, load or query path, run the work budgets and allocation checks. Never widen
   a bound to get a green build.
8. If it is a timed change, write the measurement plan down first, then measure it under the
   protocol in §6.
9. Map every requirement to its tests and every test to its requirement.
10. State the intent in the PR, and expect review against it rather than against the code.
11. For a bug: root-cause write-up, failing-first regression, lesson.
