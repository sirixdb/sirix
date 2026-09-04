/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseType;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.NamePage;
import io.sirix.settings.Constants;

/** Construction-time validation for physical HOT index reference numbers. */
final class HOTIndexNumberValidator {

  private HOTIndexNumberValidator() {
    throw new AssertionError("May never be instantiated!");
  }

  static void validate(final StorageEngineReader storageEngineReader, final IndexType indexType,
      final int indexNumber) {
    if (indexNumber < 0 || indexNumber >= Constants.INP_REFERENCE_COUNT) {
      throw new IllegalArgumentException("HOT index number outside reference space: " + indexNumber);
    }
    if (indexType != IndexType.NAME) {
      return;
    }

    final ResourceSession<?, ?> resourceSession = storageEngineReader.getResourceSession();
    final DatabaseType databaseType;
    if (resourceSession instanceof JsonResourceSession) {
      databaseType = DatabaseType.JSON;
    } else if (resourceSession instanceof XmlResourceSession) {
      databaseType = DatabaseType.XML;
    } else {
      throw new IllegalStateException("Cannot determine the database type for NAME index " + indexNumber
          + " from resource session " + resourceSession);
    }

    if (NamePage.isNameDictionarySlot(databaseType, indexNumber)) {
      throw new IllegalArgumentException(
          "NAME HOT index " + indexNumber + " collides with reserved " + databaseType + " NamePage dictionary slots");
    }
  }
}
