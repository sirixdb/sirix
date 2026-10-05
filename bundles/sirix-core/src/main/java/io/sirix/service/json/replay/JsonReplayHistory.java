package io.sirix.service.json.replay;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.node.RevisionReferencesNode;
import io.sirix.node.interfaces.DataRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;

/** Imports exact authoritative node-history events, including histories of deleted identities. */
public final class JsonReplayHistory {
  private JsonReplayHistory() {}

  public static void importChanges(final JsonNodeReadOnlyTrx source, final StorageEngineWriter target,
      final JsonReplayManifest manifest) {
    if (!source.getResourceSession().getResourceConfig().storeNodeHistory()) {
      return;
    }
    final int offset = manifest.targetRevision() - manifest.destinationRevision();
    final int start = offset + 1;
    try (final var base = source.getResourceSession().beginNodeReadOnlyTrx(manifest.baseRevision());
        final var boundary = offset == 0
            ? null
            : source.getResourceSession().beginNodeReadOnlyTrx(start)) {
      final var oldReader = base.getStorageEngineReader();
      final var newReader = source.getStorageEngineReader();
      final var oldRoot = oldReader.getActualRevisionRootPage();
      final var newRoot = newReader.getActualRevisionRootPage();
      new JsonReplayPageWalk(oldReader, newReader, IndexType.RECORD_TO_REVISIONS, 0, (key, oldExists, newExists) -> {
        if (key == 0) {
          return;
        }
        final RevisionReferencesNode old = oldExists
            ? history(oldReader, key)
            : null;
        final RevisionReferencesNode current = newExists
            ? history(newReader, key)
            : null;
        if (current == null) {
          if (old != null && target.getRecord(key, IndexType.RECORD_TO_REVISIONS, 0) != null) {
            target.removeRecord(key, IndexType.RECORD_TO_REVISIONS, 0);
          }
          return;
        }
        if (old != null && Arrays.equals(old.getRevisions(), current.getRevisions())) {
          return;
        }
        final int[] revisions = current.getRevisions();
        final int[] mapped;
        if (offset == 0) {
          mapped = revisions.clone();
        } else {
          int firstVisible = 0;
          while (firstVisible < revisions.length && revisions[firstVisible] < start) {
            firstVisible++;
          }
          final var visible = new IntArrayList(revisions.length - firstVisible + 1);
          // The copied snapshot makes an older live identity first available at revision one.
          // An actual event at that boundary already represents that availability.
          if (firstVisible > 0 && (firstVisible == revisions.length || revisions[firstVisible] > start)
              && boundary != null && boundary.moveTo(key)) {
            visible.add(1);
          }
          for (int index = firstVisible; index < revisions.length; index++) {
            visible.add(revisions[index] - offset);
          }
          mapped = visible.toIntArray();
        }
        if (mapped.length > 0) {
          target.persistRecord(new RevisionReferencesNode(key, mapped), IndexType.RECORD_TO_REVISIONS, 0);
        } else if (target.getRecord(key, IndexType.RECORD_TO_REVISIONS, 0) != null) {
          target.removeRecord(key, IndexType.RECORD_TO_REVISIONS, 0);
        }
      }).read(oldRoot.getIndirectRecordToRevisionsIndexPageReference(),
          oldRoot.getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages(),
          newRoot.getIndirectRecordToRevisionsIndexPageReference(),
          newRoot.getCurrentMaxLevelOfRecordToRevisionsIndexIndirectPages());
      target.getActualRevisionRootPage()
            .setMaxNodeKeyInRecordToRevisionsIndex(newRoot.getMaxNodeKeyInRecordToRevisionsIndex());
    }
  }

  private static RevisionReferencesNode history(final StorageEngineReader reader, final long key) {
    final DataRecord record = reader.getRecord(key, IndexType.RECORD_TO_REVISIONS, 0);
    if (record == null) {
      return null;
    }
    if (!(record instanceof RevisionReferencesNode history)) {
      throw new IllegalStateException("Invalid history record for identity " + key);
    }
    return history;
  }
}
