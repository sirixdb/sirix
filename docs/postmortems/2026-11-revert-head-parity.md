# Bug: MCP head reverts skipped the commit

- Fix / issue: captain call 9 (C3).
- Symptom: the same successful head revert appended a revision through REST/core but returned the existing head through MCP.
- Trigger: invoke `sirix_revert` with the current head as a numeric revision or snapshot target.
- Root cause: MCP added an equality shortcut before the shared revert/commit path, treating identical content as permission to omit a requested history event.
- Why tests missed it: the MCP test explicitly expected the shortcut; REST's head test only covered JSON and did not inspect persisted values across revisions.
- Regression test: `ToolHandlersIntegrationTest.Revert.revertToCurrentRevisionAppendsForNumericAndSnapshotTargets` checks returned revision metadata, persisted head, commit message and unchanged historical values for both target representations. `RevertHandlerTest.optionalHeadAndMessageAndRevertToHeadAppendARevision` checks the same history behavior for JSON and XML.
- Lesson: define revision-event semantics once and test each interface and target representation against that contract; see [Lessons](../LESSONS.md).
