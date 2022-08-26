package io.sirix.tutorial.xml;

import io.sirix.tutorial.Constants;
import org.sirix.access.Databases;
import org.sirix.access.trx.node.HashType;
import org.sirix.diff.DiffFactory;
import org.sirix.diff.DiffFactory.DiffOptimized;

import java.util.Set;

public class DiffXmlResource {
  public static void main(String[] args) {
    CreateVersionedXmlResource.createXmlDatabaseWithVersionedResource();

    diff();
  }

  private static void diff() {
    final var databaseFile = Constants.SIRIX_DATA_LOCATION.resolve("xml-database-versioned");

    try (final var database = Databases.openXmlDatabase(databaseFile);
         final var manager = database.beginResourceSession("resource");
         final var rtxOnFirstRevision = manager.beginNodeReadOnlyTrx(1);
         final var rtxOnThirdRevision = manager.beginNodeReadOnlyTrx(3)) {
      DiffFactory.invokeFullXmlDiff(new DiffFactory.Builder<>(manager,
                                                              3,
                                                              1,
                                                              manager.getResourceConfig().hashType == HashType.NONE
                                                                  ? DiffOptimized.NO
                                                                  : DiffOptimized.HASHED,
                                                              Set.of(new MyXmlDiffObserver(rtxOnThirdRevision,
                                                                                           rtxOnFirstRevision))).skipSubtrees(
          true));
    }
  }
}
