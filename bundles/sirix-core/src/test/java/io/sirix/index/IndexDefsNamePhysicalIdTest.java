/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.atomic.QNm;
import io.sirix.page.PageConstants;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests the one logical-to-physical mapping used by every NAME index factory. */
final class IndexDefsNamePhysicalIdTest {

  @Test
  void everyNameFactoryUsesTheNonOverlappingPhysicalNamespace() {
    assertEquals(PageConstants.JSON_NAME_INDEX_OFFSET, IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID());
    assertEquals(PageConstants.JSON_NAME_INDEX_OFFSET + 1,
        IndexDefs.createFilteredNameIdxDef(Set.of(new QNm("skip")), 1, IndexDef.DbType.JSON).getID());
    assertEquals(PageConstants.XML_NAME_INDEX_OFFSET + 2,
        IndexDefs.createSelectiveNameIdxDef(Set.of(new QNm("keep")), 2, IndexDef.DbType.XML).getID());
    assertEquals(7,
        IndexDefs.logicalNameIndexDefNoForPhysicalSlot(PageConstants.JSON_NAME_INDEX_OFFSET + 7, IndexDef.DbType.JSON));
    assertEquals(11,
        IndexDefs.logicalNameIndexDefNoForPhysicalSlot(PageConstants.XML_NAME_INDEX_OFFSET + 11, IndexDef.DbType.XML));
  }

  @Test
  void invalidLogicalDefinitionNumbersFailBeforeTheyCanAddressANamePage() {
    assertThrows(IllegalArgumentException.class, () -> IndexDefs.createNameIdxDef(-1, IndexDef.DbType.JSON));
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.createNameIdxDef(Constants.INP_REFERENCE_COUNT - PageConstants.JSON_NAME_INDEX_OFFSET,
            IndexDef.DbType.JSON));
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.createNameIdxDef(Integer.MAX_VALUE, IndexDef.DbType.XML));
    assertThrows(NullPointerException.class, () -> IndexDefs.createNameIdxDef(0, null));
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.logicalNameIndexDefNoForPhysicalSlot(PageConstants.JSON_NAME_INDEX_OFFSET - 1,
            IndexDef.DbType.JSON));
    assertThrows(IllegalArgumentException.class,
        () -> IndexDefs.logicalNameIndexDefNoForPhysicalSlot(Constants.INP_REFERENCE_COUNT, IndexDef.DbType.XML));
  }
}
