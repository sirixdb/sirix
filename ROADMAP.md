# Roadmap

SirixDB is currently in the **1.0.0-alpha** series. The engine and query layer are
feature-rich and well-tested; the focus now is stabilizing the public API and
fixing the rough edges real users hit, on the way to a stable **1.0.0** release.

The best way to influence this roadmap is to [open an issue](https://github.com/sirixdb/sirix/issues)
or join the discussion on [Discord](https://discord.gg/yC33wVpv7t) — concrete user
feedback is what drives prioritization.

## On the way to 1.0.0 (stable)

- **API stabilization** — finalize the embedded, REST, and Kotlin client APIs based on user feedback.
- **JSON copy operations** — copy subtrees/operations from other resources.
- **Diffing & patch files** — serialize the first revision, diff each pair of consecutive
  revisions into a patch describing the changes, and apply those patches.
- **SirixDB Kotlin client** — a first-class client for talking to the SirixDB server.
- **Web front-end** — the [web front-end](https://github.com/sirixdb/sirixdb-web-gui)
  for visualizing diffs between revisions, running queries, and browsing/updating resources.
- **Bug fixing & hardening** — driven by real-world usage.

## Near future

- **Sharding** — replicate and partition resources/databases across nodes, likely over a
  distributed transaction log (e.g. Apache BookKeeper). Planned as part of
  **sirix-enterprise**, the extension layer that adds distributed and infrastructure
  features (e.g. the io_uring and S3 storage backends) on top of the open-source core.

## Longer term

- **Full-text index** — full-text indexing and querying.
- **Other data models** — exploring storing and querying graph data.
- **Additional index structures.**
