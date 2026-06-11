package io.sirix.bench;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.IOStorage;
import io.sirix.io.StorageType;
import io.sirix.io.Writer;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.Utils;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * Storage-cost decomposition benchmark: answers <b>"where do the bytes of one tiny commit go?"</b>
 *
 * <p>Builds a resource with N single-number-field-update commits (the workload behind the
 * "10k commits = 15.9 MB &asymp; 1.6 KB/commit for an 8-byte logical change" number), then walks the
 * data file <i>structurally</i> — every record is {@code [align-pad][u32 len][payload]} starting at
 * {@link IOStorage#DATA_REGION_START} — decodes each payload's page-kind header (and, for
 * {@code KeyValueLeafPage}, the index type + slotted-page header), and attributes every byte of
 * file growth to a (page kind, commit) pair using the revision-root offsets recorded in the
 * {@code sirix.revisions} file. Diff files ({@code update-operations/}) and the revisions file
 * itself are accounted separately.
 *
 * <p>This is deliberately a <i>reader-side</i> ground truth (it measures what is actually ON DISK,
 * including the 4-byte length headers and 8-byte alignment padding the writer adds around each
 * page). Cross-check against the writer-side profiler with {@code -Dsirix.storage.profile=true}
 * ({@code io.sirix.io.file.StorageProfile} — payload bytes only, so this walker should report
 * slightly more per kind).
 *
 * <p>Feature-isolation knobs (system properties), mirroring {@link LargeHistoryBenchMain}:
 * <ul>
 *   <li>{@code -Dbench.label=...} — tag printed into every CSV line</li>
 *   <li>{@code -Dbench.storeDiffs=true|false} (default true)</li>
 *   <li>{@code -Dbench.hashKind=ROLLING|POSTORDER|NONE} (default ROLLING)</li>
 *   <li>{@code -Dbench.buildPathSummary=true|false} (default true)</li>
 *   <li>{@code -Dbench.storeNodeHistory=true|false} (default true)</li>
 *   <li>{@code -Dbench.storeChildCount=true|false} (default true)</li>
 *   <li>{@code -Dbench.versioning=SLIDING_SNAPSHOT|FULL|INCREMENTAL|DIFFERENTIAL} (default SLIDING_SNAPSHOT)</li>
 *   <li>{@code -Dbench.revisionsToRestore=N} (default 3)</li>
 * </ul>
 *
 * <p>Usage: {@code StorageCostBenchMain [numCommits=1000] [workDir=<tmp>]}
 *
 * <p>Compile/run without gradle:
 * <pre>
 *   javac --enable-preview --release 25 --add-modules jdk.incubator.vector \
 *     -cp "$(cat /tmp/sirix-test-cp.txt)" -d /tmp/wave5-a/classes \
 *     bundles/sirix-core/src/test/java/io/sirix/bench/StorageCostBenchMain.java
 *   java --enable-preview --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED \
 *     --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED \
 *     -Xms1g -Xmx4g -cp "/tmp/wave5-a/classes:$(cat /tmp/sirix-test-cp.txt)" \
 *     io.sirix.bench.StorageCostBenchMain 1000
 * </pre>
 */
public final class StorageCostBenchMain {

  private static final String INITIAL_DOC =
      "{\"counter\":0,\"label\":\"large-history-bench\",\"tags\":[\"a\",\"b\",\"c\"]}";

  /** Page-kind ids as written by PageKind (PagePersister dispatch byte). */
  private static final String[] KIND_NAMES = new String[32];

  static {
    KIND_NAMES[1] = "KeyValueLeafPage";
    KIND_NAMES[2] = "NamePage";
    KIND_NAMES[3] = "UberPage";
    KIND_NAMES[4] = "IndirectPage";
    KIND_NAMES[5] = "RevisionRootPage";
    KIND_NAMES[6] = "PathSummaryPage";
    KIND_NAMES[8] = "CASPage";
    KIND_NAMES[9] = "OverflowPage";
    KIND_NAMES[10] = "PathPage";
    KIND_NAMES[11] = "DeweyIDPage";
    KIND_NAMES[12] = "HOTLeafPage";
    KIND_NAMES[13] = "HOTIndirectPage";
    KIND_NAMES[14] = "BitmapChunkPage";
    KIND_NAMES[15] = "VectorPage";
    KIND_NAMES[16] = "ProjectionIndexPage";
  }

  /** One structurally-walked data-file record: [pad][u32 len][payload]. */
  private record Rec(long offset, int payloadLen, int grossLen, String group, int commit,
                     // KVLP extras (-1 when not a KVLP)
                     long recordPageKey, int pageRevision, int populated, int onDiskHeapSize,
                     int bodyCompressedLen, int bodyCodec, int prefixLen, int tailLen,
                     // reference-page extras (-1 when not parsed)
                     int refCount, int fragKeyCount, int delegateBytes) {
  }

  private StorageCostBenchMain() {
  }

  public static void main(final String[] args) throws Exception {
    final int numCommits = args.length > 0 ? Integer.parseInt(args[0]) : 1_000;
    final Path dbPath = (args.length > 1
        ? Paths.get(args[1])
        : Files.createTempDirectory("sirix-storagecost-")).resolve("db");
    final String resource = "bench";
    final String label = System.getProperty("bench.label", "default");

    final boolean storeDiffs = Boolean.parseBoolean(System.getProperty("bench.storeDiffs", "true"));
    final HashType hashKind = HashType.valueOf(System.getProperty("bench.hashKind", "ROLLING"));
    final boolean buildPathSummary = Boolean.parseBoolean(System.getProperty("bench.buildPathSummary", "true"));
    final boolean storeNodeHistory = Boolean.parseBoolean(System.getProperty("bench.storeNodeHistory", "true"));
    final boolean storeChildCount = Boolean.parseBoolean(System.getProperty("bench.storeChildCount", "true"));
    final VersioningType versioning =
        VersioningType.valueOf(System.getProperty("bench.versioning", "SLIDING_SNAPSHOT"));
    final int revisionsToRestore = Integer.getInteger("bench.revisionsToRestore", 3);

    System.out.printf(Locale.ROOT,
                      "# StorageCostBench label=%s commits=%d dbPath=%s%n"
                          + "# knobs: storeDiffs=%s hashKind=%s buildPathSummary=%s storeNodeHistory=%s "
                          + "storeChildCount=%s versioning=%s revisionsToRestore=%d compression=%s%n",
                      label, numCommits, dbPath, storeDiffs, hashKind, buildPathSummary, storeNodeHistory,
                      storeChildCount, versioning, revisionsToRestore,
                      System.getProperty("sirix.compression", "none"));

    // ---------------------------------------------------------------- build phase
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    final ResourceConfiguration resourceConfig;
    final long buildStart = System.nanoTime();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(revisionsToRestore)
                                                   .buildPathSummary(buildPathSummary)
                                                   .storeChildCount(storeChildCount)
                                                   .storeNodeHistory(storeNodeHistory)
                                                   .hashKind(hashKind)
                                                   .storeDiffs(storeDiffs)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(resource)) {
        resourceConfig = session.getResourceConfig();
        try (final var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(INITIAL_DOC));
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();   // object
          wtx.moveToFirstChild();   // object key "counter"
          wtx.moveToFirstChild();   // number value
          final long counterNodeKey = wtx.getNodeKey();
          for (int i = 2; i <= numCommits; i++) {
            wtx.moveTo(counterNodeKey);
            wtx.setNumberValue(i);
            wtx.commit();
          }
        }
        if (session.getMostRecentRevisionNumber() != numCommits) {
          throw new IllegalStateException("expected revision " + numCommits + ", got "
                                              + session.getMostRecentRevisionNumber());
        }
      }
    }
    final double buildSeconds = (System.nanoTime() - buildStart) / 1e9;

    // ---------------------------------------------------------------- file inventory
    final Path resourcePath = dbPath.resolve("resources").resolve(resource);
    final Path dataFile = resourcePath.resolve("data").resolve("sirix.data");
    final Path revisionsFile = resourcePath.resolve("data").resolve("sirix.revisions");
    final Path updateOpsDir = resourcePath.resolve("update-operations");

    final long dataSize = Files.size(dataFile);
    final long revisionsSize = Files.size(revisionsFile);
    long diffBytes = 0;
    long diffBytesBlockRounded = 0;
    int diffFiles = 0;
    if (Files.isDirectory(updateOpsDir)) {
      try (final var stream = Files.list(updateOpsDir)) {
        for (final Path p : stream.toList()) {
          final long size = Files.size(p);
          diffBytes += size;
          diffBytesBlockRounded += roundUp4k(size);
          diffFiles++;
        }
      }
    }
    long otherBytes = 0;
    try (final var stream = Files.walk(resourcePath)) {
      for (final Path p : stream.filter(Files::isRegularFile).toList()) {
        if (!p.equals(dataFile) && !p.equals(revisionsFile) && !p.startsWith(updateOpsDir)) {
          otherBytes += Files.size(p);
        }
      }
    }
    final long totalLogical = dataSize + revisionsSize + diffBytes + otherBytes;

    System.out.printf(Locale.ROOT,
                      "# build: %d commits in %.1fs (%.2f ms/commit)%n"
                          + "# files: data=%d B revisions=%d B diffs=%d B (%d files, 4KiB-rounded=%d B) other=%d B "
                          + "total=%d B (%.2f MB) => %.1f B/commit logical%n",
                      numCommits, buildSeconds, buildSeconds * 1000 / numCommits,
                      dataSize, revisionsSize, diffBytes, diffFiles, diffBytesBlockRounded, otherBytes,
                      totalLogical, totalLogical / (1024.0 * 1024.0), (double) totalLogical / numCommits);

    // ---------------------------------------------------------------- revision-root offsets
    // sirix.revisions layout: 4 KiB superblock, then one 32-byte record per revision
    // (u64 offset, u64 timestamp, u64 checksum, u64 reserved — little endian).
    final long[] revRootOffset = new long[numCommits + 1];
    try (final FileChannel ch = FileChannel.open(revisionsFile, StandardOpenOption.READ)) {
      final ByteBuffer buf = ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE)
                                       .order(ByteOrder.LITTLE_ENDIAN);
      for (int rev = 0; rev <= numCommits; rev++) {
        buf.clear();
        int read = 0;
        while (buf.hasRemaining()) {
          final int n = ch.read(buf, IOStorage.revisionsFileOffset(rev) + read);
          if (n < 0) {
            throw new IOException("short revisions file at revision " + rev);
          }
          read += n;
        }
        buf.flip();
        revRootOffset[rev] = buf.getLong();
      }
    }

    // ---------------------------------------------------------------- structural data-file walk
    final var pipelineComponents = resourceConfig.byteHandlePipeline.getComponents();
    final boolean identityPipeline = pipelineComponents.isEmpty();

    final List<Rec> recs = new ArrayList<>(1 << 16);
    try (final FileChannel ch = FileChannel.open(dataFile, StandardOpenOption.READ)) {
      final ByteBuffer lenBuf = ByteBuffer.allocate(4).order(ByteOrder.nativeOrder());
      long pos = IOStorage.DATA_REGION_START;
      int nextCommit = 0;
      while (pos < dataSize) {
        lenBuf.clear();
        while (lenBuf.hasRemaining()) {
          final int n = ch.read(lenBuf, pos + lenBuf.position());
          if (n < 0) {
            throw new IOException("short read of record length at " + pos);
          }
        }
        lenBuf.flip();
        final int len = lenBuf.getInt();
        if (len <= 0 || pos + 4 + len > dataSize) {
          throw new IOException("implausible record length " + len + " at offset " + pos);
        }
        final ByteBuffer payloadBuf = ByteBuffer.allocate(len);
        while (payloadBuf.hasRemaining()) {
          final int n = ch.read(payloadBuf, pos + 4 + payloadBuf.position());
          if (n < 0) {
            throw new IOException("short read of record payload at " + pos);
          }
        }
        byte[] payload = payloadBuf.array();
        if (!identityPipeline) {
          try (final var in = resourceConfig.byteHandlePipeline.deserialize(new ByteArrayInputStream(payload))) {
            payload = in.readAllBytes();
          }
        }

        // Commit attribution: the RevisionRootPage is the LAST data-region record of its commit
        // (children are serialized before their parents, the uber page goes to the beacon slots).
        while (nextCommit < numCommits && pos > revRootOffset[nextCommit]) {
          nextCommit++;
        }
        final long nextPos = align8(pos + 4 + len);
        final int gross = (int) (nextPos - pos);
        recs.add(parseRecord(payload, pos, len, gross, nextCommit));
        pos = nextPos;
      }
    }

    // ---------------------------------------------------------------- aggregate
    // group -> [pages, payloadBytes, grossBytes] over ALL commits and over the steady window.
    final int steadyStart = Math.max(2, numCommits - 499);
    final int steadyCommits = numCommits - steadyStart + 1;
    final Map<String, long[]> allByGroup = new TreeMap<>();
    final Map<String, long[]> steadyByGroup = new TreeMap<>();
    final long[] grossPerCommit = new long[numCommits + 1];
    final long[] pagesPerCommit = new long[numCommits + 1];
    for (final Rec r : recs) {
      accumulate(allByGroup, r);
      if (r.commit >= steadyStart) {
        accumulate(steadyByGroup, r);
      }
      grossPerCommit[r.commit] += r.grossLen;
      pagesPerCommit[r.commit] += 1;
    }

    long walkedGross = 0;
    for (final long[] v : allByGroup.values()) {
      walkedGross += v[2];
    }
    final long accounted = IOStorage.DATA_REGION_START + walkedGross;
    System.out.printf(Locale.ROOT,
                      "# data-file accounting: header+beacons=%d + walked=%d = %d (file=%d, delta=%d)%n",
                      IOStorage.DATA_REGION_START, walkedGross, accounted, dataSize, dataSize - accounted);

    // ---------------------------------------------------------------- report: global table
    System.out.println();
    System.out.printf(Locale.ROOT, "=== ALL commits (0..%d): on-disk bytes by page kind ===%n", numCommits);
    printGroupTable(allByGroup, numCommits, walkedGross);

    System.out.println();
    System.out.printf(Locale.ROOT, "=== STEADY-STATE commits (%d..%d, %d commits): per-commit average ===%n",
                      steadyStart, numCommits, steadyCommits);
    long steadyGross = 0;
    for (final long[] v : steadyByGroup.values()) {
      steadyGross += v[2];
    }
    printGroupTable(steadyByGroup, steadyCommits, steadyGross);

    // Non-page per-commit costs.
    final double diffPerCommit = storeDiffs && numCommits > 1 ? (double) diffBytes / diffFiles : 0;
    System.out.println();
    System.out.printf(Locale.ROOT, "non-page per-commit costs:%n");
    System.out.printf(Locale.ROOT, "  revisions-file record              %8.1f B/commit%n",
                      (double) IOStorage.REVISIONS_FILE_RECORD_SIZE);
    System.out.printf(Locale.ROOT, "  diff file (update-operations)      %8.1f B/commit logical "
                          + "(%d files, %.1f B avg, 4KiB-block cost %.0f B/commit)%n",
                      diffPerCommit, diffFiles, diffPerCommit,
                      storeDiffs && diffFiles > 0 ? (double) diffBytesBlockRounded / diffFiles : 0.0);
    final double steadyPageBytes = (double) steadyGross / steadyCommits;
    System.out.printf(Locale.ROOT,
                      "TOTAL steady-state: %.1f B/commit pages + %d B revisions + %.1f B diff = %.1f B/commit%n",
                      steadyPageBytes, IOStorage.REVISIONS_FILE_RECORD_SIZE, diffPerCommit,
                      steadyPageBytes + IOStorage.REVISIONS_FILE_RECORD_SIZE + diffPerCommit);

    // per-commit distribution
    final long[] sorted = new long[steadyCommits];
    for (int c = steadyStart; c <= numCommits; c++) {
      sorted[c - steadyStart] = grossPerCommit[c];
    }
    java.util.Arrays.sort(sorted);
    System.out.printf(Locale.ROOT,
                      "steady per-commit page bytes: min=%d p50=%d p90=%d max=%d  (pages/commit p50=%d)%n",
                      sorted[0], sorted[steadyCommits / 2], sorted[(int) (steadyCommits * 0.9)],
                      sorted[steadyCommits - 1], medianPages(pagesPerCommit, steadyStart, numCommits));

    // ---------------------------------------------------------------- example commits, full detail
    System.out.println();
    System.out.printf(Locale.ROOT, "=== record-level detail: last 3 commits (%d..%d) ===%n",
                      numCommits - 2, numCommits);
    System.out.printf(Locale.ROOT, "%-8s %-34s %9s %9s  %s%n", "commit", "page", "payload", "gross", "detail");
    for (final Rec r : recs) {
      if (r.commit >= numCommits - 2) {
        final StringBuilder detail = new StringBuilder();
        if (r.recordPageKey >= 0) {
          detail.append(String.format(Locale.ROOT,
                                      "rpk=%d pageRev=%d slots=%d heap=%dB prefix=%dB body=%dB tail=%dB codec=%d",
                                      r.recordPageKey, r.pageRevision, r.populated, r.onDiskHeapSize,
                                      r.prefixLen, r.bodyCompressedLen, r.tailLen, r.bodyCodec));
        }
        if (r.refCount >= 0) {
          detail.append(String.format(Locale.ROOT, "refs=%d fragKeys=%d delegateBytes=%d",
                                      r.refCount, r.fragKeyCount, r.delegateBytes));
        }
        System.out.printf(Locale.ROOT, "%-8d %-34s %9d %9d  %s%n",
                          r.commit, r.group, r.payloadLen, r.grossLen, detail);
      }
    }

    // ---------------------------------------------------------------- CSV (machine readable)
    System.out.println();
    for (final var e : steadyByGroup.entrySet()) {
      final long[] v = e.getValue();
      System.out.printf(Locale.ROOT, "CSVKIND,%s,%s,pages_per_commit=%.3f,payload_b_per_commit=%.1f,gross_b_per_commit=%.1f%n",
                        label, e.getKey(), (double) v[0] / steadyCommits, (double) v[1] / steadyCommits,
                        (double) v[2] / steadyCommits);
    }
    System.out.printf(Locale.ROOT,
                      "CSVTOTAL,%s,commits=%d,data_b=%d,revisions_b=%d,diff_b=%d,diff_files=%d,other_b=%d,"
                          + "total_b=%d,b_per_commit=%.1f,steady_page_b_per_commit=%.1f,steady_total_b_per_commit=%.1f%n",
                      label, numCommits, dataSize, revisionsSize, diffBytes, diffFiles, otherBytes, totalLogical,
                      (double) totalLogical / numCommits, steadyPageBytes,
                      steadyPageBytes + IOStorage.REVISIONS_FILE_RECORD_SIZE + diffPerCommit);

    // Writer-side cross-check (populated when -Dsirix.storage.profile=true).
    io.sirix.io.file.StorageProfile.dump();
  }

  // ------------------------------------------------------------------ payload parsing

  /**
   * Classify one decompressed page payload. Mirrors the V0 wire format of
   * {@code PagePersister}/{@code PageKind}: {@code [kindId][binaryVersion][kind-specific…]}.
   */
  private static Rec parseRecord(final byte[] payload, final long offset, final int len, final int gross,
      final int commit) {
    final BytesIn<?> in = Bytes.wrapForRead(payload);
    final int kindId = in.readByte() & 0xFF;
    final String kindName = kindId < KIND_NAMES.length && KIND_NAMES[kindId] != null
        ? KIND_NAMES[kindId] : ("UnknownKind" + kindId);

    long recordPageKey = -1;
    int pageRevision = -1;
    int populated = -1;
    int onDiskHeapSize = -1;
    int bodyCompressedLen = -1;
    int bodyCodec = -1;
    int prefixLen = -1;
    int tailLen = -1;
    int refCount = -1;
    int fragKeyCount = -1;
    int delegateBytes = -1;
    String group = kindName;

    try {
      final int version = in.readByte() & 0xFF; // BinaryEncodingVersion
      switch (kindId) {
        case 1 -> { // KeyValueLeafPage (slotted V0)
          recordPageKey = Utils.getVarLong(in);
          pageRevision = in.readInt();
          final IndexType indexType = IndexType.getType(in.readByte());
          in.skip(160); // header(32) + slot bitmap(128) — duplicated uncompressed on disk
          populated = in.readInt();
          onDiskHeapSize = in.readInt();
          final int templateCount = in.readByte() & 0xFF;
          if (templateCount == 0) {
            in.readInt();             // templatePoolBytes (unused on this path)
          } else {
            in.readByte();            // structuralFlags
            in.readInt();             // templatePoolBytes
          }
          bodyCompressedLen = in.readInt();
          bodyCodec = in.readByte() & 0xFF;
          // Sections AFTER the body blob: PAX region table + overlong entries + FSST table.
          prefixLen = (int) in.position();
          tailLen = payload.length - prefixLen - bodyCompressedLen;
          group = kindName + "[" + indexType + "]";
        }
        case 4 -> { // IndirectPage: [delegateType][delegate]
          final int delegateType = in.readByte() & 0xFF;
          final long before = in.position();
          final int[] counts = parseDelegate(in, delegateType);
          refCount = counts[0];
          fragKeyCount = counts[1];
          delegateBytes = (int) (in.position() - before);
        }
        case 5 -> { // RevisionRootPage: BitmapReferencesPage(10) delegate w/o type byte
          final long before = in.position();
          final int[] counts = parseDelegate(in, 1);
          refCount = counts[0];
          fragKeyCount = counts[1];
          delegateBytes = (int) (in.position() - before);
        }
        case 2, 6, 8, 10, 11 -> { // NamePage/PathSummaryPage/CASPage/PathPage/DeweyIDPage
          final int delegateType = in.readByte() & 0xFF;
          final long before = in.position();
          final int[] counts = parseDelegate(in, delegateType);
          refCount = counts[0];
          fragKeyCount = counts[1];
          delegateBytes = (int) (in.position() - before);
        }
        default -> {
          // classification by kind id is enough for the table
        }
      }
      if (version != 0) {
        group = group + "(v" + version + ")";
      }
    } catch (final RuntimeException e) {
      group = group + "(parse-error)";
    }
    return new Rec(offset, len, gross, group, commit, recordPageKey, pageRevision, populated,
                   onDiskHeapSize, bodyCompressedLen, bodyCodec, prefixLen, tailLen,
                   refCount, fragKeyCount, delegateBytes);
  }

  /**
   * Parse a references-page delegate (SerializationType.DATA wire format), returning
   * {refCount, totalFragmentKeys} and leaving the cursor after the delegate.
   */
  private static int[] parseDelegate(final BytesIn<?> in, final int delegateType) {
    int refs = 0;
    int frags = 0;
    switch (delegateType) {
      case 0 -> { // ReferencesPage4: [u8 size][refs…][size × u16 offsets]
        final int size = in.readByte() & 0xFF;
        for (int i = 0; i < size; i++) {
          frags += parseRefFragmentsAndHash(in);
        }
        in.skip(size * 2L);
        refs = size;
      }
      case 1 -> { // BitmapReferencesPage: [u16 bitsetLen][bitset][refs…]
        final int bitsetLen = in.readShort() & 0xFFFF;
        final byte[] bits = new byte[bitsetLen];
        in.read(bits);
        int cardinality = 0;
        for (final byte b : bits) {
          cardinality += Integer.bitCount(b & 0xFF);
        }
        for (int i = 0; i < cardinality; i++) {
          frags += parseRefFragmentsAndHash(in);
        }
        refs = cardinality;
      }
      case 2 -> { // FullReferencesPage: [bitset][per ref: u64 key + fragments + hash]
        final int bitsetLen = in.readShort() & 0xFFFF;
        final byte[] bits = new byte[bitsetLen];
        in.read(bits);
        int cardinality = 0;
        for (final byte b : bits) {
          cardinality += Integer.bitCount(b & 0xFF);
        }
        for (int i = 0; i < cardinality; i++) {
          in.readLong(); // key (written a second time inside parseRefFragmentsAndHash's trailer)
          frags += parseRefFragmentsAndHash(in);
        }
        refs = cardinality;
      }
      default -> throw new IllegalStateException("unknown delegate type " + delegateType);
    }
    return new int[] { refs, frags };
  }

  /** [u8 fragCount][fragCount × (u32 revision + u64 key)][u64 key][u8 hashPresent][8B hash?] */
  private static int parseRefFragmentsAndHash(final BytesIn<?> in) {
    final int fragCount = in.readByte() & 0xFF;
    in.skip(fragCount * 12L);
    in.readLong();                        // current key
    final int hasHash = in.readByte() & 0xFF;
    if (hasHash != 0) {
      in.skip(8);
    }
    return fragCount;
  }

  // ------------------------------------------------------------------ helpers

  private static void accumulate(final Map<String, long[]> map, final Rec r) {
    final long[] v = map.computeIfAbsent(r.group, k -> new long[6]);
    v[0]++;
    v[1] += r.payloadLen;
    v[2] += r.grossLen;
    if (r.prefixLen >= 0) { // KVLP section split
      v[3] += r.prefixLen;
      v[4] += r.bodyCompressedLen;
      v[5] += r.tailLen;
    }
  }

  private static void printGroupTable(final Map<String, long[]> byGroup, final int commits, final long totalGross) {
    System.out.printf(Locale.ROOT, "%-34s %8s %12s %12s %10s %12s %7s  %s%n",
                      "page kind", "pages", "payload B", "gross B", "B/page", "B/commit", "share",
                      "kvlp prefix/body/tail per page");
    for (final var e : byGroup.entrySet()) {
      final long[] v = e.getValue();
      final String split = v[3] + v[4] + v[5] > 0
          ? String.format(Locale.ROOT, "%.0f/%.0f/%.0f", (double) v[3] / v[0], (double) v[4] / v[0],
                          (double) v[5] / v[0])
          : "";
      System.out.printf(Locale.ROOT, "%-34s %8d %12d %12d %10.1f %12.1f %6.1f%%  %s%n",
                        e.getKey(), v[0], v[1], v[2], (double) v[2] / v[0], (double) v[2] / commits,
                        100.0 * v[2] / totalGross, split);
    }
    System.out.printf(Locale.ROOT, "%-34s %8s %12s %12d %10s %12.1f %7s%n",
                      "TOTAL", "", "", totalGross, "", (double) totalGross / commits, "100%");
  }

  private static long medianPages(final long[] pagesPerCommit, final int from, final int to) {
    final long[] window = java.util.Arrays.copyOfRange(pagesPerCommit, from, to + 1);
    java.util.Arrays.sort(window);
    return window[window.length / 2];
  }

  private static long align8(final long pos) {
    return (pos + Writer.PAGE_FRAGMENT_BYTE_ALIGN - 1) & ~(long) (Writer.PAGE_FRAGMENT_BYTE_ALIGN - 1);
  }

  private static long roundUp4k(final long size) {
    return Math.max(4096, (size + 4095) & ~4095L);
  }
}
