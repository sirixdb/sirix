package io.sirix.index;

import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
  void loadedDefinitionsIsolateNumericEvidenceAcrossControllers() {
    final IndexDef definition = IndexDefs.createCASIdxDef(false, Type.INT, Set.of(), 0, IndexDef.DbType.JSON);
    final Indexes first = new Indexes();
    final Indexes second = new Indexes();
    first.initFrom(1, List.of(definition));
    second.initFrom(1, List.of(definition));

    first.getIndexDef(0, IndexType.CAS).markNonNumericValue();

    assertTrue(first.differsFromPersisted());
    assertTrue(definition.hasNumericValuesOnly());
    assertTrue(definition.hasCompleteNumericCoverage());
    assertTrue(second.getIndexDef(0, IndexType.CAS).hasNumericValuesOnly());
    assertTrue(second.getIndexDef(0, IndexType.CAS).hasCompleteNumericCoverage());
    assertFalse(second.differsFromPersisted());
    first.markPersisted(2);
    assertFalse(first.differsFromPersisted());
  }

  @Test
  void creatingAndDroppingAnIndexLeavesThePersistedEmptyCatalogueUnchanged() {
    final Indexes indexes = new Indexes();
    indexes.initFrom(1, List.of());
    final IndexDef definition = IndexDefs.createCASIdxDef(false, Type.INT, Set.of(), 0, IndexDef.DbType.JSON);

    indexes.add(definition);
    indexes.removeIndex(definition);

    assertTrue(indexes.isEmpty());
    assertFalse(indexes.differsFromPersisted());
  }

  @ParameterizedTest
  @EnumSource(value = IndexDef.DbType.class)
  void equivalentRecreatedDefinitionsLeaveThePersistedCatalogueUnchanged(final IndexDef.DbType dbType) {
    final Indexes indexes = new Indexes();
    indexes.initFrom(1, List.of(IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, dbType)));

    indexes.removeIndex(indexes.getIndexDef(0, IndexType.CAS));
    indexes.add(IndexDefs.createCASIdxDef(false, Type.STR, Set.of(), 0, dbType));

    assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.CAS));
    assertFalse(indexes.differsFromPersisted());
  }

  @Test
  void numericCoverageRemainsDirtyAfterUnrelatedStructuralChangesAreUndone() {
    final Indexes indexes = new Indexes();
    indexes.initFrom(1, List.of(IndexDefs.createCASIdxDef(false, Type.INT, Set.of(), 0, IndexDef.DbType.JSON)));
    final IndexDef temporary = IndexDefs.createNameIdxDef(1, IndexDef.DbType.JSON);

    indexes.getIndexDef(0, IndexType.CAS).markIncompleteNumericCoverage();
    indexes.add(temporary);
    indexes.removeIndex(temporary);

    assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.CAS));
    assertTrue(indexes.differsFromPersisted());
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
