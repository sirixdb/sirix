package io.sirix.index;

import io.brackit.query.jdm.DocumentException;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

final class IndexesTest {

  @Test
  void persistedInitializationReplacesAnAbortedCachedCatalogue() throws DocumentException {
    final Indexes cached = new Indexes();
    cached.add(IndexDefs.createPathIdxDef(Set.of(), 1, IndexDef.DbType.JSON));

    final Indexes persisted = new Indexes();
    final IndexDef persistedName = IndexDefs.createNameIdxDef(2, IndexDef.DbType.JSON);
    persisted.add(persistedName);

    cached.init(persisted.materialize());

    assertNull(cached.getIndexDef(1, IndexType.PATH),
        "initialization retained an index definition from the abandoned transaction");
    assertNotNull(cached.getIndexDef(persistedName.getID(), IndexType.NAME));
    assertFalse(cached.isDirty(), "loading persisted metadata is not a catalogue mutation");
  }

  @Test
  void resetPublishesThePersistedEmptyCatalogue() {
    final Indexes indexes = new Indexes();
    indexes.add(IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON));

    indexes.reset();

    assertFalse(indexes.isDirty());
    assertFalse(indexes.getIndexDefs().iterator().hasNext());
  }

  @Test
  void noOpCatalogueMutationsDoNotDirtyTheCatalogue() {
    final Indexes indexes = new Indexes();
    final IndexDef definition = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    indexes.add(definition);
    indexes.clearDirty();

    indexes.add(definition);
    indexes.removeIndex(IndexDefs.createNameIdxDef(1, IndexDef.DbType.JSON));

    assertFalse(indexes.isDirty());
  }
}
