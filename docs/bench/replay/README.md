# Replay benchmark evidence

This directory retains the replay campaign evidence and its regeneration scripts outside
Gradle's disposable output directories. Run scripts from the repository root, for example:

```bash
bash docs/bench/replay/run-normal-matrix.sh
```

Matrix scripts write logs and result summaries here. The latency exporter writes its
classes, harness and classpath metadata below `latency/<variant>/`; latency campaigns
write to their supplied output directory. Regeneration can replace archived evidence,
so review those changes before committing them.

Gradle wrappers and latency forks keep private Maven repositories, Gradle homes and
temporary files in the ignored `.gradle/replay/` directory. Set `SIRIX_REPLAY_SCRATCH`
to an absolute directory to keep this scratch material elsewhere. It is separate from
the retained evidence and survives `gradlew clean`; no wrapper uses `~/.m2`.

The archived reports, manifests and logs retain their original measurements, commit
identities and digests. Path prefixes were updated during relocation; absolute paths
still describe the original machine and worktree, not portable runtime classpaths.
Regenerate exports before running latency forks. Historical commands can also refer
to untracked helpers or expired validation windows from the original campaign.
