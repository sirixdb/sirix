package io.sirix.axis.pathsummary;

import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import io.sirix.JsonTestHelper;
import io.sirix.index.path.summary.PathSummaryReader;

/**
 * Test {@link ChildAxis}.
 */
public final class ChildAxisTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createTestDocument();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  public void testChildAxis() {
    final var database = JsonTestHelper.getDatabaseWithHashesEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final PathSummaryReader pathSummaryReader = session.openPathSummary()) {
      pathSummaryReader.moveTo(1);
      final var childAxis = new ChildAxis(pathSummaryReader.getPathNode());

      while (childAxis.hasNext()) {
        final var node = childAxis.next();
        System.out.println(node.getName());
      }
    }
  }
}
