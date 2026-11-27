/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.utils.OS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MEM-106 and the access rule of MEM-104: the platform release primitive, on a real reserved
 * region, run on every CI lane (Linux x64, Linux arm64, macOS arm64, Windows). Each lane runs only
 * the assertions of its own platform:
 *
 * A pattern is written to every page of a granule-aligned run and its residency recorded; the run
 * is released; then
 *
 * <ol>
 * <li><em>immediately</em>, before any read or write of that run, residency drops by at least the
 * run size on Linux ({@code mincore}) and macOS ({@code proc_pid_rusage}'s
 * {@code ri_phys_footprint}); on Windows the working set is recorded in the test output, not
 * asserted;</li>
 * <li>a read does not fault;</li>
 * <li>Linux reads zeros; macOS and Windows read zeros or the old pattern, either is accepted;</li>
 * <li>a write after release reads back, also after a second release of a neighbouring run;</li>
 * <li>on a <em>separate</em> released, untouched macOS run, the re-charge advice
 * ({@code MADV_FREE_REUSE}) is accepted and the footprint rises again on first touch.</li>
 * </ol>
 *
 * <p>
 * The granule of a run is {@code max(slot size, OS page size)} with the smallest slot (4 KiB), so
 * the runs here are aligned to the OS page size the backend reports, never an assumed 4 KiB (macOS
 * arm64 uses 16 KiB).
 */
@MemPlanned(step = 1, value = {"MEM-106", "MEM-104"})
final class PlatformReleaseSemanticsTest {

  private static final long MIB = 1024L * 1024L;
  private static final long RUN_BYTES = 4L * MIB;
  private static final long PATTERN = 0x5A5A_C3C3_1234_5678L;
  private static final long SMALLEST_SLOT = FrameSlotAllocator.SIZE_CLASSES[0];

  private final VirtualMemory vm = VirtualMemory.forCurrentPlatform();
  private MemorySegment region;
  private long granule;
  private MemorySegment runA;
  private MemorySegment runB;
  /** Committed but never touched before its release: the macOS re-charge run. */
  private MemorySegment runC;

  @BeforeEach
  void reserveAndFill() {
    final long pageBytes = MemPlatformSeam.osPageBytes(vm);
    granule = Math.max(SMALLEST_SLOT, pageBytes);
    region = vm.reserve(4 * RUN_BYTES);
    final long offset = (granule - region.address() % granule) % granule;
    final MemorySegment area = region.asSlice(offset, 3 * RUN_BYTES);
    // Windows has no overcommit: reserved pages must be committed before the first touch.
    vm.commitFresh(area);
    runA = area.asSlice(0, RUN_BYTES);
    runB = area.asSlice(RUN_BYTES, RUN_BYTES);
    runC = area.asSlice(2 * RUN_BYTES, RUN_BYTES);
    fill(runA, PATTERN);
    fill(runB, PATTERN);
    assertEquals(0L, runA.address() % granule, "the run starts on a granule boundary");
  }

  @AfterEach
  void releaseRegion() {
    vm.release(region);
  }

  @Test
  @DisplayName("MEM-106 (2)(3): a read after release does not fault and reads what the platform allows")
  void readAfterReleaseDoesNotFaultAndFollowsThePlatformContract() {
    assertTrue(MemPlatformSeam.releaseRun(vm, runA), "the release call reports success");

    final long[] counts = classify(runA, PATTERN);
    assertEquals(0L, counts[2], "a released run reads zeros or the old pattern, never anything else");
    if (OS.isLinux()) {
      assertEquals(runA.byteSize() / Long.BYTES, counts[0], "Linux reads zeros after MADV_DONTNEED");
    }
    assertEquals(runB.byteSize() / Long.BYTES, classify(runB, PATTERN)[1], "the neighbouring run keeps its contents");
  }

  @Test
  @DisplayName("MEM-106 (4): a write after release reads back, also after releasing a neighbouring run")
  void writeAfterReleaseReadsBackAlsoAfterANeighbourIsReleased() {
    assertTrue(MemPlatformSeam.releaseRun(vm, runA));
    final long written = 0x0123_4567_89AB_CDEFL;
    fill(runA, written);
    assertEquals(runA.byteSize() / Long.BYTES, count(runA, written), "a write after release reads back");

    assertTrue(MemPlatformSeam.releaseRun(vm, runB), "the neighbouring run releases too");
    fill(runB, written + 1);
    assertEquals(runB.byteSize() / Long.BYTES, count(runB, written + 1), "a write after the second release");
    assertEquals(runA.byteSize() / Long.BYTES, count(runA, written),
        "releasing the neighbour leaves the first run's new contents alone");
  }

  @Test
  @DisplayName("MEM-104: released memory stays mapped, readable and writable for the allocator's lifetime")
  void releasedMemoryStaysAccessibleAcrossRepeatedReleases() {
    long lastWritten = PATTERN;
    for (int round = 0; round < 4; round++) {
      assertTrue(MemPlatformSeam.releaseRun(vm, runA));
      assertEquals(0L, classify(runA, lastWritten)[2], "round " + round + ": no foreign bytes");
      lastWritten = PATTERN + round;
      fill(runA, lastWritten);
      assertEquals(runA.byteSize() / Long.BYTES, count(runA, lastWritten), "round " + round + ": writable");
    }
  }

  @Test
  @DisplayName("MEM-106 (1): immediately after release, before any access, residency drops by the run size")
  void residencyDropsByAtLeastTheRunSizeBeforeAnyAccess() {
    if (OS.isWindows()) {
      final long before = workingSetBytes();
      assertTrue(MemPlatformSeam.releaseRun(vm, runA));
      final long after = workingSetBytes();
      System.out.println("MEM-106 (1) Windows working set, recorded not asserted: before=" + before + " after=" + after
          + " run=" + RUN_BYTES);
      return;
    }
    assumeTrue(OS.isLinux() || OS.isMacOSX(), "MEM-106 (1) is asserted on the Linux and macOS lanes only");
    final Residency residency = OS.isLinux()
        ? new LinuxResidency()
        : new MacFootprint();
    final long before = residency.bytes(runA);
    assertTrue(before >= RUN_BYTES - RUN_BYTES / 8, "precondition: the pattern-filled run is resident, was " + before);

    assertTrue(MemPlatformSeam.releaseRun(vm, runA));
    // No read or write of runA between the release and this measurement.
    final long after = residency.bytes(runA);

    assertTrue(before - after >= RUN_BYTES,
        "resident memory dropped by " + (before - after) + " bytes, less than the run size " + RUN_BYTES);
  }

  @Test
  @DisplayName("MEM-106 (5): on a separate released, untouched macOS run, re-charge raises the footprint on first touch")
  void macRechargeOfAnUntouchedRunRaisesTheFootprintOnFirstTouch() {
    assumeTrue(OS.isMacOSX(), "MEM-106 (5) is asserted on the macOS lane only");
    final Residency footprint = new MacFootprint();
    assertTrue(MemPlatformSeam.releaseRun(vm, runC), "the untouched run releases");
    final long released = footprint.bytes(runC);

    assertTrue(MemPlatformSeam.recharge(vm, runC), "MADV_FREE_REUSE succeeds on a released run");
    for (long offset = 0; offset < RUN_BYTES; offset += granule) {
      runC.set(ValueLayout.JAVA_LONG, offset, PATTERN);
    }

    final long touched = footprint.bytes(runC);
    assertTrue(touched - released >= RUN_BYTES / 2,
        "footprint rose by " + (touched - released) + " after first touch, run is " + RUN_BYTES);
  }

  // ===== helpers ============================================================

  private static void fill(final MemorySegment segment, final long value) {
    for (long offset = 0; offset < segment.byteSize(); offset += Long.BYTES) {
      segment.set(ValueLayout.JAVA_LONG, offset, value);
    }
  }

  private static long count(final MemorySegment segment, final long value) {
    long matches = 0L;
    for (long offset = 0; offset < segment.byteSize(); offset += Long.BYTES) {
      if (segment.get(ValueLayout.JAVA_LONG, offset) == value) {
        matches++;
      }
    }
    return matches;
  }

  /** @return {zero words, old-pattern words, any other words} */
  static long[] classify(final MemorySegment segment, final long lastWritten) {
    final long[] counts = new long[3];
    for (long offset = 0; offset < segment.byteSize(); offset += Long.BYTES) {
      final long word = segment.get(ValueLayout.JAVA_LONG, offset);
      counts[word == 0L
          ? 0
          : word == lastWritten
              ? 1
              : 2]++;
    }
    return counts;
  }

  private interface Residency {
    /** Resident bytes of {@code run} (Linux), or the process footprint (macOS; {@code run} unused). */
    long bytes(MemorySegment run);
  }

  /** Linux: the pages of the run that {@code mincore} reports resident. */
  private final class LinuxResidency implements Residency {
    private final MethodHandle mincore =
        Linker.nativeLinker()
              .downcallHandle(Linker.nativeLinker().defaultLookup().find("mincore").orElseThrow(),
                  FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                      ValueLayout.ADDRESS));

    @Override
    public long bytes(final MemorySegment run) {
      final long page = MemPlatformSeam.osPageBytes(vm);
      final long length = run.byteSize();
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment vector = arena.allocate(length / page);
        final int rc = (int) mincore.invokeExact(run, length, vector);
        assertEquals(0, rc, "mincore succeeds");
        long resident = 0L;
        for (long index = 0; index < length / page; index++) {
          if ((vector.get(ValueLayout.JAVA_BYTE, index) & 1) != 0) {
            resident += page;
          }
        }
        return resident;
      } catch (final Throwable e) {
        throw new AssertionError(e);
      }
    }
  }

  /** macOS: {@code ri_phys_footprint} of {@code proc_pid_rusage(RUSAGE_INFO_V0)}. */
  private static final class MacFootprint implements Residency {
    private static final int RUSAGE_INFO_V0 = 0;
    private static final long PHYS_FOOTPRINT_OFFSET = 72L;
    private final MethodHandle rusage;

    MacFootprint() {
      final Optional<MemorySegment> symbol = Linker.nativeLinker().defaultLookup().find("proc_pid_rusage");
      assumeTrue(symbol.isPresent(), "MEM-106 (1)(5): proc_pid_rusage is not resolvable on this macOS lane");
      rusage = Linker.nativeLinker()
                     .downcallHandle(symbol.get(), FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT,
                         ValueLayout.JAVA_INT, ValueLayout.ADDRESS));
    }

    @Override
    public long bytes(final MemorySegment run) {
      try (Arena arena = Arena.ofConfined()) {
        final MemorySegment buffer = arena.allocate(512);
        final int rc = (int) rusage.invokeExact((int) ProcessHandle.current().pid(), RUSAGE_INFO_V0, buffer);
        assertEquals(0, rc, "proc_pid_rusage succeeds");
        return buffer.get(ValueLayout.JAVA_LONG, PHYS_FOOTPRINT_OFFSET);
      } catch (final Throwable e) {
        throw new AssertionError(e);
      }
    }
  }

  /** Windows: {@code WorkingSetSize} of {@code K32GetProcessMemoryInfo}, recorded only. */
  private static long workingSetBytes() {
    try (Arena arena = Arena.ofConfined()) {
      final SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32", arena);
      final MethodHandle processHandle = Linker.nativeLinker()
                                               .downcallHandle(kernel32.find("GetCurrentProcess").orElseThrow(),
                                                   FunctionDescriptor.of(ValueLayout.ADDRESS));
      final MethodHandle info =
          Linker.nativeLinker()
                .downcallHandle(kernel32.find("K32GetProcessMemoryInfo").orElseThrow(), FunctionDescriptor.of(
                    ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
      final MemorySegment counters = arena.allocate(80);
      counters.set(ValueLayout.JAVA_INT, 0, 72);
      final int ok = (int) info.invokeExact((MemorySegment) processHandle.invokeExact(), counters, 72);
      return ok == 0
          ? -1L
          : counters.get(ValueLayout.JAVA_LONG, 16);
    } catch (final Throwable e) {
      return -1L;
    }
  }
}
