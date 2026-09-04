/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.PageConstants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** Fail-closed coverage for direct HOT NAME construction outside {@code IndexDefs}. */
final class HOTIndexNumberValidatorTest {

  @Test
  void jsonAndXmlReservedNamePageSlotsAreRejected() {
    final StorageEngineReader jsonReader = mock(StorageEngineReader.class);
    doReturn(mock(JsonResourceSession.class)).when(jsonReader).getResourceSession();
    assertThrows(IllegalArgumentException.class,
        () -> HOTIndexNumberValidator.validate(jsonReader, IndexType.NAME, PageConstants.JSON_NAME_INDEX_OFFSET - 1));
    assertDoesNotThrow(
        () -> HOTIndexNumberValidator.validate(jsonReader, IndexType.NAME, PageConstants.JSON_NAME_INDEX_OFFSET));

    final StorageEngineReader xmlReader = mock(StorageEngineReader.class);
    doReturn(mock(XmlResourceSession.class)).when(xmlReader).getResourceSession();
    assertThrows(IllegalArgumentException.class,
        () -> HOTIndexNumberValidator.validate(xmlReader, IndexType.NAME, PageConstants.XML_NAME_INDEX_OFFSET - 1));
    assertDoesNotThrow(
        () -> HOTIndexNumberValidator.validate(xmlReader, IndexType.NAME, PageConstants.XML_NAME_INDEX_OFFSET));
  }
}
