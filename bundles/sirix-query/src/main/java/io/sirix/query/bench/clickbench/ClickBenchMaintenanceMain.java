package io.sirix.query.bench.clickbench;

import com.sun.management.HotSpotDiagnosticMXBean;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.jdm.Sequence;
import io.sirix.HftBoundaryTelemetry;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexChangeListener;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.io.SharedArenas;
import io.sirix.io.StorageType;
import io.sirix.node.ValueDictionaryHeaderNode;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Locale;

/** Runs a bounded ordinary-maintenance arm against an existing AUTO-global ClickBench store. */
public final class ClickBenchMaintenanceMain {

  private ClickBenchMaintenanceMain() {
    throw new AssertionError("no instances");
  }

  public static void main(final String[] args) {
    if (args.length < 3 || args.length > 4) {
      throw new IllegalArgumentException(
          "Usage: ClickBenchMaintenanceMain <dbDir> <baseRows> <dirtyRecords> [autoCommitNodes]");
    }
    final Path root = Path.of(args[0]);
    final long baseRows = positiveLong(args[1], "baseRows");
    final int dirtyRecords = positiveInt(args[2], "dirtyRecords");
    final int autoCommitNodes = args.length == 4
        ? positiveInt(args[3], "autoCommitNodes")
        : 16_384;
    if (dirtyRecords < 100_001 || dirtyRecords > baseRows) {
      throw new IllegalArgumentException("dirtyRecords must be in [100001, baseRows]");
    }
    if (!Boolean.getBoolean("sirix.hft.telemetry")) {
      throw new IllegalStateException("-Dsirix.hft.telemetry=true is required");
    }
    if (!"auto".equalsIgnoreCase(System.getProperty("sirix.projection.globalDict", "auto"))) {
      throw new IllegalStateException("-Dsirix.projection.globalDict=auto is required");
    }

    Allocators.getInstance().init(Long.parseLong(System.getProperty("sirix.offheap.bytes", String.valueOf(8L << 30))));
    final Path databasePath = root.resolve(ClickBenchSchema.DATABASE);
    final DictionaryFixture fixture = readFixture(databasePath, dirtyRecords);
    final HftRuntimeEvidence.Build hftBuild = HftRuntimeEvidence.capture(ClickBenchMaintenanceMain.class);
    final String config = hftConfiguration(fixture, baseRows, autoCommitNodes, hftBuild);

    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    GlobalValueDictionary.resetProbeTelemetry();
    System.out.println("# HFT_MEASURE_START");
    System.out.println("# HFT_BUILD gitSha=" + hftBuild.gitSha() + " artifactSha256=" + hftBuild.artifactSha256());
    System.out.println(config);
    System.out.flush();

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE);
        final JsonNodeTrx wtx = session.beginNodeTrx(autoCommitNodes, AfterCommitState.KEEP_OPEN_ASYNC_COMMIT)) {
      if (!wtx.moveToDocumentRoot() || !wtx.moveToFirstChild() || !wtx.moveToFirstChild()) {
        throw new IllegalStateException("ClickBench resource does not contain a non-empty record array");
      }
      for (int i = 0; i < dirtyRecords; i++) {
        final long recordKey = wtx.getNodeKey();
        moveToField(wtx, fixture.fieldName());
        wtx.setStringValue(i == fixture.existingValueUpdateIndex()
            ? fixture.oldValue()
            : "sirix-maintenance-" + i);
        if (!wtx.moveTo(recordKey) || (i + 1 < dirtyRecords && !wtx.moveToRightSibling())) {
          throw new IllegalStateException("ClickBench resource ended before dirty record " + (i + 1));
        }
      }
      wtx.commit();
    }

    final HftBoundaryTelemetry.Snapshot maintenanceBoundaries = HftBoundaryTelemetry.snapshot();
    final VerificationEvidence evidence = verifyColdState(databasePath, fixture, baseRows, dirtyRecords);
    ProjectionIndexChangeListener.printMaintenanceTelemetry(maintenanceBoundaries);
    System.out.printf(Locale.ROOT,
        "# HFT_PROJECTION_EVIDENCE revisionsVerified=%d historicalRevisions=%d "
            + "oracleRows=%d servedRows=%d oracleMatches=%d servedMatches=%d servedRevisions=%d "
            + "stableAnchors=%d stableIds=%d " + "successorSegments=%d introductionRevision=%d maxProbeUnits=%d%n",
        evidence.revisionsVerified(), evidence.revisionsVerified() - 1, evidence.oracleRows(), evidence.servedRows(),
        evidence.oracleMatches(), evidence.servedMatches(), evidence.servedRevisions(), evidence.stableAnchors()
            ? 1
            : 0,
        evidence.stableIds()
            ? 1
            : 0,
        evidence.successorSegments(), evidence.introductionRevision(), GlobalValueDictionary.maxProbeUnits());
    System.out.println("# HFT_MEASURE_END");
    System.out.flush();
  }

  private static DictionaryFixture readFixture(final Path databasePath, final int dirtyRecords) {
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      if (session.getResourceConfig().storageType != StorageType.FILE_CHANNEL) {
        throw new IllegalStateException("maintenance HFT arm requires FILE_CHANNEL storage");
      }
      final ProjectionIndexMetadata metadata = readMetadata(rtx);
      final byte[] kinds = metadata.columnKinds();
      final String[] names = metadata.fieldNames();
      int column = -1;
      for (int i = 0; i < kinds.length; i++) {
        if (kinds[i] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
          column = i;
          break;
        }
      }
      if (column < 0) {
        throw new IllegalStateException("AUTO did not elect a global ClickBench projection column");
      }
      if (!rtx.moveToDocumentRoot() || !rtx.moveToFirstChild() || !rtx.moveToFirstChild()) {
        throw new IllegalStateException("ClickBench resource does not contain a non-empty record array");
      }
      long recordKey = rtx.getNodeKey();
      moveToField(rtx, names[column]);
      final String oldValue = rtx.getValue();
      int existingValueUpdateIndex = -1;
      for (int i = 1; i < dirtyRecords - 1; i++) {
        if (!rtx.moveTo(recordKey) || !rtx.moveToRightSibling()) {
          break;
        }
        recordKey = rtx.getNodeKey();
        moveToField(rtx, names[column]);
        if (!oldValue.equals(rtx.getValue())) {
          existingValueUpdateIndex = i;
          break;
        }
      }
      if (existingValueUpdateIndex < 0) {
        throw new IllegalStateException("AUTO-global fixture has no different value to update to an existing id");
      }
      final long anchor = metadata.valueDictionaryHeaderKey(column);
      final int oldId =
          GlobalValueDictionary.probe(anchor, oldValue.getBytes(StandardCharsets.UTF_8), rtx.getStorageEngineReader());
      if (anchor <= 0 || oldId <= 0) {
        throw new IllegalStateException("global dictionary fixture has no stable anchor/id");
      }
      return new DictionaryFixture(names[column], column, anchor, oldValue, oldId, existingValueUpdateIndex,
          session.getResourceConfig().versioningType.name(), session.getMostRecentRevisionNumber());
    }
  }

  private static VerificationEvidence verifyColdState(final Path databasePath, final DictionaryFixture fixture,
      final long baseRows, final int dirtyRecords) {
    final int latestRevision;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE)) {
      latestRevision = session.getMostRecentRevisionNumber();
    }
    if (latestRevision <= fixture.baselineRevision()) {
      throw new IllegalStateException("ordinary maintenance created no successor revision");
    }
    long oracleRows = 0L;
    long servedRows = 0L;
    long oracleMatches = 0L;
    long servedMatches = 0L;
    int servedRevisions = 0;
    int introductionRevision = 0;
    int maximumSuccessorSegments = 0;
    boolean stableAnchors = true;
    boolean stableIds = true;
    for (int revision = fixture.baselineRevision(); revision <= latestRevision; revision++) {
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
      try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          final JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
        try {
          final ProjectionIndexMetadata metadata = readMetadata(rtx);
          if (session.getRtxIndexController(revision)
                     .openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"},
                         new String[] {fixture.fieldName()}) == null) {
            throw new IllegalStateException("cold-reopen projection route declined revision " + revision);
          }
          stableAnchors &= metadata.valueDictionaryHeaderKey(fixture.column()) == fixture.anchor();
          final int oldId = GlobalValueDictionary.probe(fixture.anchor(),
              fixture.oldValue().getBytes(StandardCharsets.UTF_8), rtx.getStorageEngineReader());
          final int firstNewId = GlobalValueDictionary.probe(fixture.anchor(),
              "sirix-maintenance-0".getBytes(StandardCharsets.UTF_8), rtx.getStorageEngineReader());
          stableIds &= oldId == fixture.oldId();
          if (firstNewId > 0 && introductionRevision == 0) {
            introductionRevision = revision;
          }
          final ValueDictionaryHeaderNode header =
              GlobalValueDictionary.header(fixture.anchor(), rtx.getStorageEngineReader());
          final int successorSegments = header == null
              ? 0
              : header.getGeneration();
          maximumSuccessorSegments = Math.max(maximumSuccessorSegments, successorSegments);
          if (revision == latestRevision) {
            final int lastNewId = GlobalValueDictionary.probe(fixture.anchor(),
                ("sirix-maintenance-" + (dirtyRecords - 1)).getBytes(StandardCharsets.UTF_8),
                rtx.getStorageEngineReader());
            if (firstNewId <= 0 || lastNewId <= firstNewId) {
              throw new IllegalStateException("new dictionary ids are not durable in the latest revision");
            }
            if (!rtx.moveToDocumentRoot() || !rtx.moveToFirstChild() || !rtx.moveToFirstChild()) {
              throw new IllegalStateException("ClickBench resource does not contain a non-empty record array");
            }
            for (int i = 0; i < fixture.existingValueUpdateIndex(); i++) {
              if (!rtx.moveToRightSibling()) {
                throw new IllegalStateException("existing-value update record is absent after cold reopen");
              }
            }
            moveToField(rtx, fixture.fieldName());
            if (!fixture.oldValue().equals(rtx.getValue())) {
              throw new IllegalStateException("existing dictionary value update was not durable");
            }
          }
          final String firstNewValue = "sirix-maintenance-0";
          final String lastNewValue = "sirix-maintenance-" + (dirtyRecords - 1);
          final RevisionOracle oracle =
              fieldOracle(rtx, fixture.fieldName(), fixture.oldValue(), firstNewValue, lastNewValue);
          final long servedBefore = SirixVectorizedExecutor.projectionCountsServed();
          final Sequence count = executor.executeAggregate(null, new String[] {"[]"}, "count", fixture.fieldName());
          if (!(count instanceof final Int64 value) || value.longValue() != oracle.rows()) {
            throw new IllegalStateException("projection count differs from record oracle at revision " + revision);
          }
          final long matches = servedPredicateCount(executor, fixture.fieldName(), fixture.oldValue())
              + servedPredicateCount(executor, fixture.fieldName(), firstNewValue)
              + servedPredicateCount(executor, fixture.fieldName(), lastNewValue);
          if (matches != oracle.matches() || SirixVectorizedExecutor.projectionCountsServed() - servedBefore < 3L) {
            throw new IllegalStateException(
                "projection predicates differ from the record oracle at revision " + revision);
          }
          oracleRows += oracle.rows();
          servedRows += value.longValue();
          oracleMatches += oracle.matches();
          servedMatches += matches;
          servedRevisions++;
          System.out.printf(Locale.ROOT,
              "# HFT_PROJECTION_REVISION revision=%d oracleRows=%d servedRows=%d "
                  + "oracleMatches=%d servedMatches=%d anchor=%d oldId=%d newId=%d successorSegments=%d%n",
              revision, oracle.rows(), value.longValue(), oracle.matches(), matches,
              metadata.valueDictionaryHeaderKey(fixture.column()), oldId, firstNewId, successorSegments);
        } finally {
          executor.close();
        }
      }
    }
    if (!stableAnchors || !stableIds || introductionRevision <= fixture.baselineRevision()
        || maximumSuccessorSegments < 3) {
      throw new IllegalStateException("cold historical dictionary evidence is incomplete");
    }
    if (oracleRows < baseRows || servedRows != oracleRows || servedMatches != oracleMatches) {
      throw new IllegalStateException("cold historical projection evidence is inconsistent");
    }
    final int revisionsVerified = latestRevision - fixture.baselineRevision() + 1;
    return new VerificationEvidence(revisionsVerified, oracleRows, servedRows, oracleMatches, servedMatches,
        servedRevisions, stableAnchors, stableIds, maximumSuccessorSegments, introductionRevision);
  }

  private static RevisionOracle fieldOracle(final JsonNodeReadOnlyTrx rtx, final String fieldName,
      final String oldValue, final String firstNewValue, final String lastNewValue) {
    if (!rtx.moveToDocumentRoot() || !rtx.moveToFirstChild() || !rtx.moveToFirstChild()) {
      return new RevisionOracle(0L, 0L);
    }
    long rows = 0L;
    long matches = 0L;
    do {
      final long recordKey = rtx.getNodeKey();
      moveToField(rtx, fieldName);
      final String value = rtx.getValue();
      rows++;
      if (oldValue.equals(value)) {
        matches++;
      }
      if (firstNewValue.equals(value)) {
        matches++;
      }
      if (lastNewValue.equals(value)) {
        matches++;
      }
      if (!rtx.moveTo(recordKey)) {
        throw new IllegalStateException("record disappeared while counting field " + fieldName);
      }
    } while (rtx.moveToRightSibling());
    return new RevisionOracle(rows, matches);
  }

  private static long servedPredicateCount(final SirixVectorizedExecutor executor, final String fieldName,
      final String value) {
    final Sequence count =
        executor.executePredicateCount(null, new String[] {"[]"}, new PredicateNode.StrEq(fieldName, value));
    if (!(count instanceof final Int64 matches)) {
      throw new IllegalStateException("projection declined equality for " + fieldName);
    }
    return matches.longValue();
  }

  private static ProjectionIndexMetadata readMetadata(final JsonNodeReadOnlyTrx rtx) {
    final ProjectionIndexMetadata metadata =
        ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), 0, 0L));
    if (metadata == null || metadata.isStale()) {
      throw new IllegalStateException("ClickBench projection metadata is absent or stale");
    }
    return metadata;
  }

  private static void moveToField(final JsonNodeReadOnlyTrx trx, final String fieldName) {
    if (!trx.moveToFirstChild()) {
      throw new IllegalStateException("record has no fields");
    }
    do {
      if (trx.getName() != null && fieldName.equals(trx.getName().getLocalName())) {
        return;
      }
    } while (trx.moveToRightSibling());
    throw new IllegalStateException("record has no field " + fieldName);
  }

  private static String hftConfiguration(final DictionaryFixture fixture, final long baseRows,
      final int autoCommitNodes, final HftRuntimeEvidence.Build hftBuild) {
    return String.format(Locale.ROOT,
        "# HFT_CONFIG globalDict=auto autoCommitNodes=%d asyncFlushNodeCap=0 arenaStrategy=%s maxNewSizeBytes=%d "
            + "initialHeapBytes=%d maxHeapBytes=%d g1RegionSizeBytes=%d gcLogging=%s safepointLogging=%s "
            + "storage=FILE_CHANNEL importer=ordinary-maintenance projectionMode=incremental expectedRows=%d "
            + "pinnedTrieScanBudget=%d "
            + "pinnedTrieBatchCapacity=%d versioningType=%s appendWorkers=%d appendQueueCapacity=%d",
        autoCommitNodes, SharedArenas.strategy().name().toLowerCase(Locale.ROOT), effectiveVmOption("MaxNewSize"),
        effectiveVmOption("InitialHeapSize"), effectiveVmOption("MaxHeapSize"), effectiveVmOption("G1HeapRegionSize"),
        hftBuild.gcLogging(), hftBuild.safepointLogging(), baseRows,
        Integer.getInteger("sirix.asyncFlush.pinnedTrieSpillScanBudget", 1_024),
        Integer.getInteger("sirix.asyncFlush.pinnedTrieSpillBatchCapacity", 64), fixture.versioningType(),
        appendWorkers(), positiveProperty("sirix.asyncFlush.appendQueueCapacity", 1));
  }

  private static long effectiveVmOption(final String name) {
    return Long.parseLong(
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).getVMOption(name).getValue());
  }

  private static int appendWorkers() {
    final int defaultWorkers = Math.min(2, Math.max(1, Runtime.getRuntime().availableProcessors() / 4));
    return positiveProperty("sirix.asyncFlush.appendParallelism", defaultWorkers);
  }

  private static int positiveProperty(final String name, final int defaultValue) {
    final int value = Integer.getInteger(name, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException("-D" + name + " must be positive");
    }
    return value;
  }

  private static int positiveInt(final String value, final String name) {
    final int parsed = Integer.parseInt(value);
    if (parsed <= 0) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return parsed;
  }

  private static long positiveLong(final String value, final String name) {
    final long parsed = Long.parseLong(value);
    if (parsed <= 0L) {
      throw new IllegalArgumentException(name + " must be positive");
    }
    return parsed;
  }

  private record DictionaryFixture(String fieldName, int column, long anchor, String oldValue, int oldId,
      int existingValueUpdateIndex, String versioningType, int baselineRevision) {
  }

  private record VerificationEvidence(int revisionsVerified, long oracleRows, long servedRows, long oracleMatches,
      long servedMatches, int servedRevisions, boolean stableAnchors, boolean stableIds, int successorSegments,
      int introductionRevision) {
  }

  private record RevisionOracle(long rows, long matches) {
  }
}
