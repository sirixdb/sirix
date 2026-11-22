<!--
Thanks for contributing to SirixDB! Please fill in the sections below.
Keep the PR focused — smaller PRs get reviewed faster.
-->

## What does this PR do?

<!-- A short summary of the change and the motivation behind it. -->

## Related issue

<!-- e.g. "Closes #123". If there's no issue, briefly explain the context. -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behavior)
- [ ] Performance improvement
- [ ] Documentation only

## Checklist

- [ ] `./gradlew build` passes locally (JDK 25)
- [ ] Added or updated tests covering the change
- [ ] For behavioral changes to the storage engine: the relevant invariant in
      [`docs/formal-verification.md`](../docs/formal-verification.md) is still upheld (and updated/added if needed)
- [ ] Updated documentation (README / docs) where relevant
- [ ] No unrelated formatting churn (run Spotless / the `pre-commit` hook)

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

## Notes for reviewers

<!-- Anything that needs special attention: trade-offs, follow-ups, areas of uncertainty. -->
