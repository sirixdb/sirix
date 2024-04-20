package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Axis;
import io.sirix.axis.DescendantAxis;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.util.concurrent.TimeUnit;

public final class JsonNodeTrxAsyncCommitTest {
  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  @RepeatedTest(1)
  public void testFullVersioning() {
    test(VersioningType.FULL, 1024, 409600);
  }

  @RepeatedTest(1)
  public void testFullVersioningSimple1() {
    testSimple(VersioningType.FULL, 1024, 2048);
  }

  @RepeatedTest(1)
  public void testFullVersioningSimple2() {
    testSimple(VersioningType.FULL, 1024, 409600);
  }

  @RepeatedTest(1)
  public void testDifferentialVersioning() {
    test(VersioningType.DIFFERENTIAL, 1024, 409600);
  }

  @RepeatedTest(1)
  public void testDifferentialVersioningSimple() {
    testSimple(VersioningType.DIFFERENTIAL, 1024, 40960);
  }

  @RepeatedTest(1)
  public void testIncrementalVersioning1() {
    test(VersioningType.INCREMENTAL, 1024, 409600);
  }

  @RepeatedTest(1)
  public void testIncrementalVersioningSimple() {
    testSimple(VersioningType.INCREMENTAL, 1024, 4096000);
  }

  @RepeatedTest(1)
  public void testSlidingSnapshotVersioning1() {
    test(VersioningType.SLIDING_SNAPSHOT, 1024, 2050);
  }

  @RepeatedTest(1)
  public void testSlidingSnapshotVersioning2() {
    test(VersioningType.SLIDING_SNAPSHOT, 1024, 409600);
  }

  private void testSimple(VersioningType versioningType, int maxNodeCount, long iterationCount) {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(true)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   // .byteHandlerPipeline(new ByteHandlerPipeline())
                                                   .versioningApproach(versioningType)
                                                   .build());
      try (final var manager = database.beginResourceSession(resource);
           final var wtx = manager.beginNodeTrx(maxNodeCount, 0, TimeUnit.SECONDS, true)) {
        wtx.insertArrayAsFirstChild();
        for (int i = 0; i < iterationCount; i++) {
          wtx.moveTo(1);
          wtx.insertObjectAsFirstChild();
        }
        wtx.commit();

        wtx.moveToDocumentRoot();

        for (Axis axis = new DescendantAxis(wtx); axis.hasNext(); axis.nextLong()) {
          System.out.println(axis.getTrx().getNode());
        }
      }
    }
  }

  private void test(VersioningType versioningType, int maxNodeCount, long iterationCount) {
    final var resource = "smallInsertions";

    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioningType)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final var manager = database.beginResourceSession(resource);
           final var wtx = manager.beginNodeTrx(maxNodeCount, 0, TimeUnit.SECONDS, true)) {
        wtx.insertArrayAsFirstChild();
        for (int i = 0; i < iterationCount; i++) {
          wtx.moveTo(1);
          wtx.insertObjectAsFirstChild();
          var nodeKey = wtx.getNodeKey();
          wtx.insertObjectRecordAsFirstChild("foo", new StringValue("bar"));
          wtx.moveTo(nodeKey);
          wtx.insertObjectRecordAsFirstChild("baz", new StringValue("bar"));
          wtx.moveTo(nodeKey);
          wtx.insertObjectRecordAsFirstChild("hurray", new BooleanValue(true));
        }
        for (int i = 0; i < iterationCount; i++) {
          wtx.moveTo(1);
          wtx.insertObjectAsFirstChild();
          var nodeKey = wtx.getNodeKey();
          wtx.insertObjectRecordAsFirstChild("tada", new StringValue("bar"));
          wtx.moveTo(nodeKey);
          wtx.insertObjectRecordAsFirstChild("todo", new StringValue("bar"));
          wtx.moveTo(nodeKey);
          wtx.insertObjectRecordAsFirstChild("tidi", new BooleanValue(true));
        }
        wtx.commit();

        wtx.moveToDocumentRoot();

        int i = 0;

        for (Axis axis = new DescendantAxis(wtx); axis.hasNext(); axis.nextLong()) {
          i++;

          if (i % 1000 == 0) {
            System.out.println(axis.getTrx().getNode());
          }
        }
      }
    }
  }
}
