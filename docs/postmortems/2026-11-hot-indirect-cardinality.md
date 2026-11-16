# Bug: HOT indirect cardinality contracts disagreed

- Fix / issue: captain call 6 (H10).
- Symptom: MultiNode factories accepted one child although I10 and the validator require at least two; persisted corrupt counts reached layout decoding and allocation before validation.
- Trigger: create a single-child MultiNode in either mask layout, or decode an indirect header with child count 1.
- Root cause: the lower bound was implemented independently by factories, node-type selection, structural detection and writer guards, with inconsistent domains. The persisted count had no early validation.
- Why tests missed it: factory tests exercised ordinary fanouts; traversal-budget fixtures even used unary nodes as convenient malformed shapes, implicitly relying on the permissive factory.
- Regression test: `HOTIndirectCardinalityTest` executes every factory and COW form, round-trips legal minimum fanouts, supplies truncated corrupt headers for both layouts and node types, and injects malformed in-memory cardinalities into detection and writer preflight. `NodeUpgradeManagerTest` rejects one child. Budget fixtures use binary chains so they still exercise page limits and cycles.
- Lesson: enforce the same structural domain at construction, decoding and publication boundaries; see [Lessons](../LESSONS.md).
