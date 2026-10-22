# Replay benchmark evidence

This directory retains the replay campaign evidence and its regeneration scripts outside
Gradle's disposable output directories. Run scripts from the repository root, for example:

```bash
bash docs/bench/replay/run-normal-matrix.sh
```

Matrix scripts write logs and result summaries here. The latency exporter writes its
classes, harness and classpath metadata below `latency/<variant>/`; latency campaigns
write logs to their supplied output directory and database fixtures to `latency/data/<label>/`.
Regeneration can replace archived evidence, so review those changes before committing them.

Gradle wrappers and latency forks keep private Maven repositories, Gradle homes and
JVM temporary directories in the ignored `.gradle/replay/` directory. This scratch material is
separate from the retained evidence and survives `gradlew clean`; no wrapper uses `~/.m2`.

The archived reports, manifests and logs retain their original measurements, commit
identities and digests. Path prefixes were updated during relocation; absolute paths
still describe the original machine and worktree, not portable runtime classpaths.
Regenerate exports before running latency forks. Only the tracked subset of campaign
evidence is retained here; references to other raw logs, JUnit XML and result directories
describe campaign-local captures that are absent from a fresh checkout. Historical commands
can also refer to untracked helpers or expired validation windows from the original campaign.
