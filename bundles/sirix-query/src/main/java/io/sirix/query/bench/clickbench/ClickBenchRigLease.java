package io.sirix.query.bench.clickbench;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/** Process-lifetime Linux flock leases for the benchmark, outside query execution. */
final class ClickBenchRigLease implements AutoCloseable {
  private static final String CAMPAIGN_DIRECTORY = "CB100M_DIR";
  private static final String RIG_WORK = "CB_RIG_WORK";
  private static final String CLASSIFICATION = "CB_RIG_CLASSIFICATION";
  private static final String CLASSIFIED_DATABASE = "CB_RIG_CLASSIFIED_DB";
  private static final String CAMPAIGN = "campaign";
  private static final String POINTER_FILE = "current-100m-dir.txt";
  private static final String RIG_MARKER = "bundles/sirix-query/bench/clickbench/rig/rig.env";
  private static final String DEFAULT_WORK = "bundles/sirix-query/build/diagnostics/rig";
  private static final String UNSET = "unset";
  private static final long GIB = 1L << 30;
  private static final int LOCK_SH = 1;
  private static final int LOCK_EX = 2;
  private static final int LOCK_NB = 4;
  private static final int O_RDWR = 2;
  private static final int O_CREAT = 64;
  private static final int O_NOFOLLOW = 131072;
  private static final int O_CLOEXEC = 524288;
  private static final int F_SETFD = 2;
  private static final int FD_CLOEXEC = 1;

  // Keep leases until OS process exit, including when main fails but worker threads survive.
  // A shutdown hook or a main-method finally block could release them before the JVM exits.
  private static List<ClickBenchRigLease> processLeases;
  private int descriptor;

  private ClickBenchRigLease(final int descriptor) {
    this.descriptor = descriptor;
  }

  /** One directory named as the campaign 100M database, and the source that named it. */
  record CampaignPointer(String named, String source) {
  }

  /**
   * Whether a run's database is the campaign 100M one, which directory was named as that database
   * ({@code unset} when none was), and what settled it: a pointer source, or the rig launcher whose
   * lease this process runs under.
   */
  record Decision(boolean campaign, String named, String source) {
  }

  /**
   * The rig working directory holding the campaign pointer file, resolved as rig.env resolves it. A
   * rig-launched JVM inherits {@code CB_RIG_WORK}; a raw one finds the enclosing checkout, so the two
   * entry points read the same file.
   */
  static Path rigWork() {
    final String configured = System.getenv(RIG_WORK);
    if (configured != null && !configured.isBlank()) {
      return Path.of(configured.strip());
    }
    final Path start = Path.of("").toAbsolutePath();
    for (Path directory = start; directory != null; directory = directory.getParent()) {
      if (Files.isRegularFile(directory.resolve(RIG_MARKER))) {
        return directory.resolve(DEFAULT_WORK);
      }
    }
    return start.resolve(DEFAULT_WORK);
  }

  static List<CampaignPointer> campaignPointers() {
    return campaignPointers(rigWork());
  }

  /**
   * Every place the rig names the campaign 100M database, most authoritative first: the pointer file
   * the campaign load rewrites on every reload, then {@code CB100M_DIR}. This is the same chain and
   * the same precedence the Python rig applies; see bench/clickbench/rig/README.md.
   */
  static List<CampaignPointer> campaignPointers(final Path work) {
    final List<CampaignPointer> found = new ArrayList<>(2);
    final Path pointer = work.resolve(POINTER_FILE);
    try {
      final String named = Files.readString(pointer).strip();
      if (!named.isEmpty()) {
        found.add(new CampaignPointer(named, pointer.toString()));
      }
    } catch (final IOException absent) {
      // No pointer file is ordinary on a box that never loaded the campaign corpus.
    }
    final String variable = System.getenv(CAMPAIGN_DIRECTORY);
    if (variable != null && !variable.isBlank()) {
      found.add(new CampaignPointer(variable.strip(), CAMPAIGN_DIRECTORY));
    }
    return found;
  }

  /**
   * This run's campaign-identity decision. A rig launcher resolves identity once and exports the
   * conclusion it reached; this process honours that conclusion and consults no pointer at all, so it
   * cannot reach a different answer than the parent whose lease it holds. Deriving the answer is what
   * a raw {@code java -cp} or Gradle run does, having no parent that decided for it.
   */
  static Decision decide(final Path database) throws IOException {
    final Decision inherited =
        inheritedDecision(System.getenv(CLASSIFICATION), System.getenv(CLASSIFIED_DATABASE), database);
    return inherited != null
        ? inherited
        : decide(campaignPointers(), database);
  }

  /** The decision the documented pointer chain implies; the one place identity is derived. */
  static Decision decide(final List<CampaignPointer> consulted, final Path database) throws IOException {
    final CampaignPointer matched = campaignMatch(consulted, database);
    final CampaignPointer announced = matched != null
        ? matched
        : consulted.isEmpty()
            ? null
            : consulted.get(0);
    return new Decision(matched != null, announced == null
        ? UNSET
        : announced.named(),
        announced == null
            ? UNSET
            : announced.source());
  }

  /**
   * The classification a rig launcher already took, or {@code null} when this process is the one
   * deciding. It is honoured only for the database it names: a value held over from another target,
   * or hand-set, must never silently reclassify this run. A non-campaign decision names no campaign
   * directory, exactly as a derived one does not.
   */
  static Decision inheritedDecision(final String decided, final String named, final Path database) throws IOException {
    if (decided == null || decided.isBlank() || named == null || named.isBlank()
        || !sameDatabase(Path.of(named.strip()), database)) {
      return null;
    }
    final boolean campaign = CAMPAIGN.equals(decided.strip());
    return new Decision(campaign, campaign
        ? named.strip()
        : UNSET, CLASSIFICATION);
  }

  /**
   * The pointer naming {@code database} as the campaign 100M database, or {@code null}. A match
   * against any consulted source wins, including a stale one, so a rotated pointer can never demote a
   * campaign run to a shared lease.
   */
  static CampaignPointer campaignMatch(final List<CampaignPointer> consulted, final Path database) throws IOException {
    for (final CampaignPointer pointer : consulted) {
      if (isCampaignDatabase(pointer.named(), database)) {
        return pointer;
      }
    }
    return null;
  }

  /**
   * Whether {@code database} is the campaign database named by {@code campaign}. Identity decides
   * while both exist; otherwise the resolved paths do, because the campaign load names its target
   * before creating it. A stale or removed campaign pointer answers {@code false} rather than
   * failing: {@link Files#isSameFile} throws when either operand is absent, and an unrelated small
   * run must not die on a variable it never used.
   */
  static boolean isCampaignDatabase(final String campaign, final Path database) throws IOException {
    if (campaign == null || campaign.isBlank() || database == null) {
      return false;
    }
    return sameDatabase(Path.of(campaign).resolve("db"), database);
  }

  /**
   * The name an identity comparison must use: absolute, with every symlink resolved as far as the
   * path exists. Operators keep a stable alias pointing at whichever campaign directory the last load
   * wrote, so an unresolved alias is a name that can come to mean a different database than the one
   * it named when a decision was reached about it. This is {@code runtime.canonical}.
   */
  static Path canonical(final Path path) {
    final Path absolute = path.toAbsolutePath();
    for (Path existing = absolute; existing != null; existing = existing.getParent()) {
      try {
        return existing.toRealPath().resolve(existing.relativize(absolute)).normalize();
      } catch (final IOException uncreated) {
        // The campaign load names its target before creating it; canonicalise what does exist.
      }
    }
    return absolute.normalize();
  }

  /** Whether two paths name one database, by the rule {@link #isCampaignDatabase} documents. */
  static boolean sameDatabase(final Path left, final Path right) throws IOException {
    if (left == null || right == null) {
      return false;
    }
    final Path first = canonical(left);
    final Path second = canonical(right);
    if (Files.exists(first) && Files.exists(second)) {
      return Files.isSameFile(first, second);
    }
    return first.equals(second);
  }

  static void holdForQueryProcess(final long arenaBytes, final Path database) throws IOException {
    holdForQueryProcess(decide(database), arenaBytes, database);
  }

  /**
   * Exclusivity follows the database, never the JVM's size: only a query against the campaign 100M
   * database takes the host lease alone, and only that run must match the 100M envelope.
   */
  static void holdForQueryProcess(final Decision decision, final long arenaBytes, final Path database)
      throws IOException {
    if (decision.campaign()) {
      validateQueryEnvelope(arenaBytes);
    }
    holdForProcess(decision, database, true);
  }

  static void holdForLoadProcess(final Path database) throws IOException {
    holdForLoadProcess(decide(database), database);
  }

  /**
   * Only the campaign 100M load is exclusive; a 1M or unrelated load shares the host lease so the
   * parallel validation lanes keep running.
   */
  static void holdForLoadProcess(final Decision decision, final Path database) throws IOException {
    // Existing load wrappers retain their legacy shell lease. The JVM owns the host lease,
    // so losing that shell cannot expose a still-running loader to another large JVM.
    holdForProcess(decision, database, false);
  }

  private static void holdForProcess(final Decision decision, final Path database, final boolean includeLegacy)
      throws IOException {
    final boolean exclusive = decision.campaign();
    if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux")) {
      if (exclusive) {
        System.out.println("# rig lease: NOT exclusive — process-owned flock leases require Linux. This run "
            + "against the campaign database is not protected against a concurrent benchmark, and is not "
            + "rig evidence.");
      }
      return;
    }
    if (processLeases != null) {
      throw new IllegalStateException("benchmark rig lease already acquired in this process");
    }
    final List<ClickBenchRigLease> acquired = new ArrayList<>(2);
    try {
      final String uid = Integer.toUnsignedString(Native.uid());
      final Path hostLock = Path.of("/tmp", "sirix-clickbench-" + uid + ".lock");
      acquired.add(acquire(hostLock, exclusive, System.getenv("CB_RIG_HOST_LOCK_FD")));
      if (exclusive) {
        refuseLegacyBenchmarkProcess();
      }
      final String work = System.getenv("CB_RIG_WORK");
      if (exclusive && includeLegacy && work != null && !work.isBlank()) {
        final Path legacyLock = Path.of(work).resolve("leg.lock");
        acquired.add(acquire(legacyLock, true, System.getenv("CB_RIG_LEGACY_LOCK_FD")));
      }
      processLeases = acquired;
      // Both operands of the classification, plus what settled it: a shared mode against a 100M
      // database means nothing placed it, and only the trio shows why. `via` names either the
      // pointer source that derived the answer here or the rig launcher that decided it upstream.
      System.out.printf("# rig lease: pid=%d mode=%s db=%s campaign=%s via=%s host=%s%n", ProcessHandle.current().pid(),
          exclusive
              ? "exclusive"
              : "shared",
          database.toAbsolutePath().normalize(), decision.named(), decision.source(), hostLock);
    } catch (final IOException | RuntimeException | Error failure) {
      for (final ClickBenchRigLease lease : acquired) {
        try {
          lease.close();
        } catch (final IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  private static void refuseLegacyBenchmarkProcess() throws IOException {
    final long current = ProcessHandle.current().pid();
    final String runMain = ClickBenchRunMain.class.getName();
    final String loadMain = ClickBenchLoadMain.class.getName();
    final String[] empty = new String[0];
    try (Stream<ProcessHandle> processes = ProcessHandle.allProcesses()) {
      for (final ProcessHandle process : processes.toList()) {
        if (process.pid() == current) {
          continue;
        }
        for (final String argument : process.info().arguments().orElse(empty)) {
          if (argument.equals(runMain) || argument.equals(loadMain)) {
            throw new IOException("A benchmark JVM is already alive: pid=" + process.pid()
                + "; legacy binaries must also run under the shared rig launcher");
          }
        }
      }
    }
  }

  private static void validateQueryEnvelope(final long arenaBytes) throws IOException {
    final HotSpotDiagnosticMXBean vm = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    if (vm == null || arenaBytes != 10 * GIB || Long.parseLong(vm.getVMOption("InitialHeapSize").getValue()) != 6 * GIB
        || Long.parseLong(vm.getVMOption("MaxHeapSize").getValue()) != 14 * GIB
        || !"false".equals(vm.getVMOption("UseJVMCICompiler").getValue())
        || Long.parseLong(System.getProperty("sirix.projection.eagerMaterializeBytes", "0")) != 5 * GIB) {
      throw new IOException("100M query envelope must be -Xms6g -Xmx14g, 10 GiB arena, 5 GiB eager residency, "
          + "and -XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler; use the rig launcher");
    }
  }

  static ClickBenchRigLease acquire(final Path path, final boolean exclusive, final String inherited)
      throws IOException {
    if (inherited != null) {
      final int fd;
      try {
        fd = Integer.parseInt(inherited);
      } catch (final NumberFormatException failure) {
        throw new IOException("Invalid inherited rig descriptor: " + inherited, failure);
      }
      if (fd < 3 || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
          || !Files.isSameFile(Path.of("/proc/self/fd/" + fd), path)) {
        throw new IOException("Inherited rig descriptor does not refer to " + path);
      }
      final String info = Files.readString(Path.of("/proc/self/fdinfo/" + fd));
      final boolean owned = info.lines()
                                .anyMatch(line -> line.startsWith("lock:") && line.contains("FLOCK")
                                    && (line.contains("WRITE") || !exclusive && line.contains("READ")));
      if (!owned) {
        throw new IOException("Inherited descriptor has no sufficient kernel flock lease: " + path);
      }
      // Descriptor flags are local to this process; do not convert or unlock the shared flock.
      Native.closeOnExec(fd);
      return new ClickBenchRigLease(fd);
    }
    final int fd = Native.open(path);
    if (fd < 0) {
      throw new IOException("Cannot open rig lock " + path);
    }
    final ClickBenchRigLease lease = new ClickBenchRigLease(fd);
    try {
      if (Native.flock(fd, (exclusive
          ? LOCK_EX
          : LOCK_SH) | LOCK_NB) != 0) {
        throw new IOException("Rig busy or flock unavailable: " + path
            + "; inspect live owners with lslocks. An empty lock file is not a blocker. "
            + "Legacy shell wrappers must use the rig launcher to pass their lease to the JVM.");
      }
    } catch (final IOException | RuntimeException | Error failure) {
      try {
        lease.close();
      } catch (final IOException closeFailure) {
        failure.addSuppressed(closeFailure);
      }
      throw failure;
    }
    return lease;
  }

  @Override
  public void close() throws IOException {
    if (descriptor >= 0) {
      final int fd = descriptor;
      descriptor = -1;
      // Close only: LOCK_UN would also unlock a parent's inherited open-file description.
      if (Native.close(fd) != 0) {
        throw new IOException("Cannot close rig descriptor " + fd);
      }
    }
  }

  private static final class Native {
    private static final Linker LINKER = Linker.nativeLinker();
    private static final MethodHandle OPEN = call("open",
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
        Linker.Option.firstVariadicArg(2));
    private static final MethodHandle FLOCK =
        call("flock", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
    private static final MethodHandle CLOSE =
        call("close", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
    private static final MethodHandle FCNTL = call("fcntl",
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT),
        Linker.Option.firstVariadicArg(2));
    private static final MethodHandle GETUID = call("getuid", FunctionDescriptor.of(ValueLayout.JAVA_INT));

    private static MethodHandle call(final String symbol, final FunctionDescriptor descriptor,
        final Linker.Option... options) {
      return LINKER.downcallHandle(LINKER.defaultLookup().find(symbol).orElseThrow(), descriptor, options);
    }

    static int open(final Path path) throws IOException {
      try (var arena = Arena.ofConfined()) {
        final MemorySegment name = arena.allocateFrom(path.toAbsolutePath().toString());
        return (int) OPEN.invokeExact(name, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0600);
      } catch (final Throwable failure) {
        throw new IOException("Native rig lock open failed", failure);
      }
    }

    static int flock(final int fd, final int mode) throws IOException {
      try {
        return (int) FLOCK.invokeExact(fd, mode);
      } catch (final Throwable failure) {
        throw new IOException("Native flock failed", failure);
      }
    }

    static int close(final int fd) throws IOException {
      try {
        return (int) CLOSE.invokeExact(fd);
      } catch (final Throwable failure) {
        throw new IOException("Native descriptor close failed", failure);
      }
    }

    static void closeOnExec(final int fd) throws IOException {
      try {
        final int result = (int) FCNTL.invokeExact(fd, F_SETFD, FD_CLOEXEC);
        if (result != 0) {
          throw new IOException("Cannot set close-on-exec on rig descriptor " + fd);
        }
      } catch (final Throwable failure) {
        throw new IOException("Native descriptor flag update failed", failure);
      }
    }

    static int uid() throws IOException {
      try {
        return (int) GETUID.invokeExact();
      } catch (final Throwable failure) {
        throw new IOException("Cannot identify rig user", failure);
      }
    }
  }
}
