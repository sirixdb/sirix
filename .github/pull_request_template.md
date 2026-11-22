## Change

What problem does this solve, and what does the caller observe now?

## Spec

Write this before implementation. Map behaviour, invariants and error cases to tests (or
observable acceptance checks for non-code changes).

| Requirement / invariant / error case | Test or acceptance check | Result |
| --- | --- | --- |
| | | |

## Mutation testing

- Changed classes and PIT report/artifact:
- Resolution of surviving or uncovered mutants under [CONV-103](../spec/CONV-conventions.md#conv-103--specification-before-implementation):
- Incomplete evidence (timeouts, missing reports, unsupported scope) and follow-up:

## Exploration

- Adversarial, property-based or differential cases beyond the happy path:
- Seeds / reproduction and outcomes:

## Bug learning (when fixing a bug)

Link the root-cause write-up, failing-before regression test and `docs/LESSONS.md` entry.

## Vision check

Test this change against the [vision](../STRATEGY.md#vision). For each rule - each `###` heading of
the Vision section - give one verdict, **aligns**, **does not align** or **cannot tell**, and the
evidence for it from the actual diff: a file, a hunk or a test, not the description above. Where the
diff does not reach a rule, the verdict is aligns and the evidence names what it leaves alone.

| Rule of the vision | Verdict | Evidence from the diff |
| --- | --- | --- |
| The advantages to protect | | |
| History is the data model | | |
| Speed is part of correctness | | |
| A fast answer is the same answer | | |
| The specification decides behaviour | | |
| One format, one route | | |
| A library first | | |
| Scope | | |

Contract class (exactly one):

- [ ] **restore** - brings behaviour back to, or leaves it at, what the specification and the
      released contract already say (a bug fix, a refactoring, documentation, CI).
- [ ] **new-default** - changes what an existing user gets without asking for it: a result, an
      error, persisted bytes, cost, a default or a public signature.
- [ ] **opt-in** - new behaviour that is reached only through an explicit setting that is off by
      default, or through a new API; every default path is unchanged.
- [ ] **cannot-tell**

A verdict of **does not align** or **cannot tell**, or a contract class of **new-default** or
**cannot-tell**, needs the maintainer's explicit decision before merge.

## Validation

Commands/checks and results, including required allocation/work budgets for hot-path changes:
