# Bug: native-header dependencies rejected by the Sonar security gate

- Fix / issue: PR #1448; Sonar rules `githubactions:S8541` and `githubactions:S8544`.
- Symptom: analysis uploads successfully, but both Sonar checks fail because the new-code security rating is C instead of A.
- Trigger: the native-header workflow installs `tests/native/requirements.txt` without wheel-only or hash-checking enforcement.
- Root cause: exact version pins were mistaken for artifact verification. Pip could select source distributions and accepted package bytes without checking an independently recorded digest.
- Why tests missed it: the header consumers check ABI declarations after installation; they do not exercise dependency artifact rejection.
- Regression checks: execute pip against the locked requirements, then repeat with altered wheel bytes and with only a source distribution available. The independent oracle is the published PyPI SHA-256 metadata plus pip's wheel-only/hash-checking contract. Acceptance of altered bytes or source distributions is the wrong behavior these checks reject. Record results below.
- Lesson: version pins, artifact hashes and wheel-only selection serve separate purposes. See [Lessons](../LESSONS.md).

## Behaviour, invariants and error cases

Every Python dependency installed for the native-header check must be an exact-version wheel whose SHA-256 matches the lock. This applies to both cffi and its pycparser dependency, the CI install command and the README install command. Retain the existing versions and all Python 3.12 wheel platforms; do not allow source-build fallback. Missing wheels, missing hashes and mismatched bytes must fail installation before header checks run.

Derive acceptance checks before changing the install configuration: use real pip to install the full lock into a worktree-local target, import both packages, and execute the existing C/C++ and cffi consumers. Against a local wheelhouse, replace a wheel with a valid repackaged wheel of the same name: the former unhashed requirements accept it, while the lock must reject it. Against a source-only package location, wheel-only resolution must fail. Also mutate an approved hash and omit a dependency's hash to check rejection. No source-text assertion can establish these behaviors.

## Caller and failure-path trace

Both the workflow and the documented local command pass wheel-only selection and required hashes to pip before reading the same requirements file. Cffi and its transitive pycparser requirement are fully pinned there. On Python 3.12, ordinary resolution selects a platform wheel and the pure-Python wheel, verifies both digests, and installs the same versions as before. A source-only candidate is filtered out; a wheel with unknown bytes or an omitted hash is rejected. CI shell failure stops the step before the header checker. Successful installation still reaches the unchanged native-header consumer command and artifact upload. No helper, branch or parameter becomes unreachable, and no fallback is added.

## Verification results

- Python 3.12.3 / pip 24.0 downloaded and installed both locked wheels successfully. Commands: `python3 -m pip download --only-binary :all: --require-hashes -r tests/native/requirements.txt --dest build/ci-native-dependencies/wheels`, then `python3 -m pip install --only-binary :all: --require-hashes --ignore-installed --no-index --find-links build/ci-native-dependencies/wheels --target build/ci-native-dependencies/packages -r tests/native/requirements.txt`. Both imported versions match the original pins.
- Six pip acceptance checks passed; logs and exit codes are in `build/ci-native-dependencies/results.json` and sibling logs. The regression fixture adds an inert file to a valid pycparser wheel without changing its name/version. With `--dry-run --ignore-installed --no-index --find-links` pointing at that wheelhouse, the old requirements accept it (exit 0); the repaired install rejects its bytes (exit 1). Mutating pycparser's approved digest or removing its hash also fails (exit 1). A source-only wheelhouse containing the published pycparser sdist is rejected even with that sdist's correct hash (exit 1), proving wheel-only selection separately from hash checking. These execute pip's public interface; no source-text assertions were added.
- Using the freshly installed packages through `PYTHONPATH`, the existing `check_native`, `check_cffi` and `check_mutations` consumers passed: C11, C++17/C linkage, cffi layout/signature checks and all eight header mutations. Generated consumer evidence is in `build/ci-native-dependencies/consumers`. `bindgen` is absent from PATH, so the Rust consumer was not rerun locally; no tool was installed or searched for outside the worktree.
- No application logic changed, so JVM PIT and read/query work budgets are outside this fix. The dependency-policy adversarial cases above exercise altered artifacts, a mutated approved hash, an omitted transitive hash and source-only resolution. The outer executor must rerun SonarCloud to confirm the remote quality gate; this phase did not upload an analysis or alter gate policy.
