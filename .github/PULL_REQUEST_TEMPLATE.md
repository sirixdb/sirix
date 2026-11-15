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

Complete the [Vision check in the contribution template](https://github.com/sirixdb/sirix/blob/main/.github/pull_request_template.md#vision-check),
including its rule-by-rule evidence, contract classification and required maintainer decisions.

## Notes for reviewers

<!-- Anything that needs special attention: trade-offs, follow-ups, areas of uncertainty. -->

## Behavior and evidence

- Specification, invariants and error cases:
- Requirement → executable test / independent oracle:
- Bug root cause, regression and lesson:
- Adversarial cases and outcomes:
- Changed-code PIT report; individual surviving/uncovered mutation rationale:
- Incomplete evidence (timeouts, missing reports, unavailable checks):
- For hot paths: allocation/work budgets and compiled-path evidence:
