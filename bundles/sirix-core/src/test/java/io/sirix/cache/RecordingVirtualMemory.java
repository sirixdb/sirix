/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The recording {@link VirtualMemory} fake of the MEM chapter's step-1 tests (MEM-101, MEM-102,
 * MEM-103, MEM-104, MEM-105, MEM-107). It records every OS-shaped call in order, can inject
 * failures, reports a configurable OS page size, and lets a test look at the allocator from inside
 * a release call.
 *
 * <p>
 * The chapter fixes behavior, not method names, so the fake is a dynamic proxy over whatever
 * {@link VirtualMemory} declares, and classifies calls by shape. It delegates every call to the
 * real backend of the platform, so the memory it hands out is real and a release really releases;
 * only the recording, the injected failures and the page size are the fake's own.
 *
 * <ul>
 * <li>{@code reserve(long)}: records the region. The first seven reservations are the seven size
 * classes, in class order. The fake aligns every region to {@value #REGION_ALIGNMENT} bytes, so a
 * granule-aligned run is aligned in absolute address whatever page size the test configures.</li>
 * <li>{@code commitFresh(MemorySegment)}, {@code populate(MemorySegment)}: recorded.</li>
 * <li>{@code release(MemorySegment)} on a whole region: shutdown. Every call after the last region
 * went back is counted in {@link #callsAfterRegionsReleased()}.</li>
 * <li>A no-argument method whose name contains {@code page} and {@code size} or {@code bytes}: the
 * OS page size; answered with {@link #osPageBytes()}.</li>
 * <li>Any other method whose first parameter is a {@link MemorySegment}: a <em>release run</em>
 * ({@link Kind#RELEASE_RUN}), or, when its name contains {@code recharge}, {@code reuse} or
 * {@code recommit}, <em>re-charge advice</em> ({@link Kind#RECHARGE}). A run is the segment itself
 * (a slice of one size class's region).</li>
 * <li>A new method of any other shape is delegated and recorded as {@link Kind#OTHER}.</li>
 * </ul>
 */
final class RecordingVirtualMemory {

  /** Alignment of the regions this fake hands out: larger than any page size a test configures. */
  static final long REGION_ALIGNMENT = 2L * 1024 * 1024;

  /** What a recorded call was. */
  enum Kind {
    RESERVE, COMMIT_FRESH, POPULATE, RELEASE_REGION, RELEASE_RUN, RECHARGE, OTHER
  }

  /** How an injected failure shows. */
  enum Failure {
    /** The call returns its failure value: {@code false}, or {@code -1} for a numeric result. */
    RETURN_FAILURE,
    /** The call throws a {@link RuntimeException}. */
    THROW
  }

  /**
   * One recorded call. {@code classIdx}, {@code firstSlot} and {@code slotCount} locate a run in a
   * size class.
   */
  record Call(int seq, Kind kind, String method, long address, long bytes, int classIdx, long firstSlot, long slotCount,
      boolean injectedFailure) {
  }

  /** Looks at the allocator from inside a release run, before the OS call is made. */
  @FunctionalInterface
  interface RunObserver {
    void beforeRun(Call run);
  }

  private final long osPageBytes;
  private final VirtualMemory real = VirtualMemory.forCurrentPlatform();
  private final List<Call> calls = new CopyOnWriteArrayList<>();
  private final List<MemorySegment> regions = new CopyOnWriteArrayList<>();
  private final List<MemorySegment> regionBackings = new CopyOnWriteArrayList<>();
  private final AtomicInteger seq = new AtomicInteger();
  private final AtomicInteger regionsReleased = new AtomicInteger();
  private final AtomicInteger callsAfterRegionsReleased = new AtomicInteger();
  private final AtomicInteger failReleaseRuns = new AtomicInteger();
  private final AtomicInteger failRecharges = new AtomicInteger();
  private final AtomicLong osEffectSuppressed = new AtomicLong();
  private volatile Failure releaseFailure = Failure.RETURN_FAILURE;
  private volatile Failure rechargeFailure = Failure.RETURN_FAILURE;
  private volatile RunObserver observer;
  private volatile boolean suppressOsEffect;
  private volatile boolean unmapped;
  private volatile int refuseReserveAt = -1;
  private final Map<Integer, Long> capSlots = new HashMap<>();
  private long nextVirtualAddress = 0x4000_0000_0000L;
  private final VirtualMemory proxy;

  private RecordingVirtualMemory(final long osPageBytes) {
    if (osPageBytes < 4096L || Long.bitCount(osPageBytes) != 1) {
      throw new IllegalArgumentException("the OS page size must be a power of two of at least 4 KiB: " + osPageBytes);
    }
    this.osPageBytes = osPageBytes;
    this.proxy = (VirtualMemory) Proxy.newProxyInstance(VirtualMemory.class.getClassLoader(),
        new Class<?>[] {VirtualMemory.class}, new Handler());
  }

  /**
   * A fake reporting {@code osPageBytes} as the OS page size (4 KiB, 16 KiB and 64 KiB occur in the
   * field).
   */
  static RecordingVirtualMemory withOsPageBytes(final long osPageBytes) {
    return new RecordingVirtualMemory(osPageBytes);
  }

  /** The {@link VirtualMemory} to hand to the allocator under test. */
  VirtualMemory virtualMemory() {
    return proxy;
  }

  long osPageBytes() {
    return osPageBytes;
  }

  /** Every call so far, in order. */
  List<Call> calls() {
    return new ArrayList<>(calls);
  }

  /** The calls of one kind, in order. */
  List<Call> callsOf(final Kind kind) {
    final List<Call> result = new ArrayList<>();
    for (final Call call : calls) {
      if (call.kind() == kind) {
        result.add(call);
      }
    }
    return result;
  }

  /** The number of calls recorded so far; use as a mark for {@link #callsSince(int)}. */
  int mark() {
    return calls.size();
  }

  /** The calls recorded after {@code mark}. */
  List<Call> callsSince(final int mark) {
    final List<Call> all = calls();
    return all.subList(mark, all.size());
  }

  /** The reserved region of size class {@code classIdx}. */
  MemorySegment region(final int classIdx) {
    return regions.get(classIdx);
  }

  /** Calls made after the last size-class region was given back. Must stay zero (MEM-107 d). */
  int callsAfterRegionsReleased() {
    return callsAfterRegionsReleased.get();
  }

  /** Make the next {@code n} release runs fail the way {@code mode} says (MEM-107 a). */
  void failNextReleaseRuns(final int n, final Failure mode) {
    releaseFailure = mode;
    failReleaseRuns.set(n);
  }

  /** Make the next {@code n} re-charge advice calls fail the way {@code mode} says (MEM-107 c). */
  void failNextRecharges(final int n, final Failure mode) {
    rechargeFailure = mode;
    failRecharges.set(n);
  }

  /**
   * Hand out address ranges without mapping anything: for tests of capacity arithmetic that would
   * otherwise reserve hundreds of GiB of address space (MEM-109). Everything the allocator does to
   * such a region reports success and touches no memory, so nothing may read or write a slot.
   */
  void withoutMapping() {
    this.unmapped = true;
  }

  /**
   * Make the reservation of size class {@code classIdx} fail like a refused {@code mmap} (MEM-109).
   */
  void refuseReservationOf(final int classIdx) {
    this.refuseReserveAt = classIdx;
  }

  /**
   * Bounded-capacity fake: the reservation of size class {@code classIdx} returns a region of only
   * {@code slots} slots, whatever size was asked for (MEM-103 slot relief, MEM-108, MEM-202). The
   * allocator must derive the class's capacity from the region it was given.
   */
  void capClassSlots(final int classIdx, final long slots) {
    if (slots < 1L) {
      throw new IllegalArgumentException("a class needs at least one slot: " + slots);
    }
    capSlots.put(classIdx, slots);
  }

  /** The byte sizes asked of {@code reserve}, in order: the first seven are the size classes. */
  List<Long> reservedBytes() {
    final List<Long> sizes = new ArrayList<>();
    for (final Call call : calls) {
      if (call.kind() == Kind.RESERVE) {
        sizes.add(call.bytes());
      }
    }
    return sizes;
  }

  /** Observe the allocator from inside every release run, before its OS call. */
  void observeRuns(final RunObserver runObserver) {
    this.observer = runObserver;
  }

  /**
   * Do not forward release runs to the OS: the fake reports success and the memory keeps its
   * contents. For tests that assert the allocator's protocol, not the OS's behavior.
   */
  void suppressOsEffect(final boolean suppress) {
    this.suppressOsEffect = suppress;
  }

  /** Release runs and re-charge advice whose OS effect was suppressed. */
  long suppressedOsEffects() {
    return osEffectSuppressed.get();
  }

  private final class Handler implements InvocationHandler {
    @Override
    public Object invoke(final Object self, final Method method, final Object[] args) throws Throwable {
      final String name = method.getName();
      final Class<?>[] types = method.getParameterTypes();
      if (method.getDeclaringClass() == Object.class) {
        return switch (name) {
          case "toString" -> "RecordingVirtualMemory[" + osPageBytes + "]";
          case "hashCode" -> System.identityHashCode(self);
          case "equals" -> self == args[0];
          default -> throw new UnsupportedOperationException(name);
        };
      }
      if (types.length == 0 && isPageSizeQuery(name)) {
        final Class<?> result = method.getReturnType();
        return result == int.class
            ? (Object) (int) osPageBytes
            : (Object) osPageBytes;
      }
      if (types.length == 1 && types[0] == long.class && name.equals("reserve")) {
        return reserve((long) args[0], method);
      }
      if (types.length >= 1 && types[0] == MemorySegment.class) {
        final MemorySegment segment = (MemorySegment) args[0];
        return switch (name) {
          case "commitFresh" -> record(method, args, Kind.COMMIT_FRESH, segment);
          case "populate" -> record(method, args, Kind.POPULATE, segment);
          case "release" -> releaseRegion(method, args, segment);
          default -> isRechargeName(name)
              ? recharge(method, args, segment)
              : releaseRun(method, args, segment);
        };
      }
      return delegate(method, args);
    }

    private Object reserve(final long asked, final Method method) throws Throwable {
      final int index = regions.size();
      if (index == refuseReserveAt) {
        calls.add(new Call(seq.getAndIncrement(), Kind.RESERVE, method.getName(), 0L, asked, index, -1, 0, true));
        throw new OutOfMemoryError("mmap failed for " + asked + " bytes");
      }
      final Long cap = capSlots.get(index);
      final long bytes = cap == null
          ? asked
          : cap * FrameSlotAllocator.SIZE_CLASSES[index];
      final MemorySegment backing;
      final MemorySegment region;
      if (unmapped) {
        final long address = nextVirtualAddress;
        nextVirtualAddress += (bytes + (1L << 30) - 1L) / (1L << 30) * (1L << 30);
        backing = MemorySegment.ofAddress(address).reinterpret(bytes);
        region = backing;
      } else {
        backing = (MemorySegment) delegate(method, new Object[] {bytes + REGION_ALIGNMENT});
        final long address = backing.address();
        final long aligned = (address + REGION_ALIGNMENT - 1L) & -REGION_ALIGNMENT;
        region = backing.asSlice(aligned - address, bytes);
      }
      regionBackings.add(backing);
      regions.add(region);
      calls.add(
          new Call(seq.getAndIncrement(), Kind.RESERVE, method.getName(), region.address(), asked, -1, -1, 0, false));
      return region;
    }

    private Object releaseRegion(final Method method, final Object[] args, final MemorySegment segment)
        throws Throwable {
      final int index = regions.indexOf(segment);
      if (index < 0) {
        // Not one of the size-class regions: a scratch or slab region this fake does not own.
        return record(method, args, Kind.OTHER, segment);
      }
      calls.add(new Call(seq.getAndIncrement(), Kind.RELEASE_REGION, method.getName(), segment.address(),
          segment.byteSize(), index, 0, 0, false));
      final Object result = delegate(method, new Object[] {regionBackings.get(index)});
      regionsReleased.incrementAndGet();
      return result;
    }

    private Object record(final Method method, final Object[] args, final Kind kind, final MemorySegment segment)
        throws Throwable {
      if (regionsReleased.get() >= regions.size() && !regions.isEmpty()) {
        callsAfterRegionsReleased.incrementAndGet();
      }
      calls.add(new Call(seq.getAndIncrement(), kind, method.getName(), segment.address(), segment.byteSize(), -1, -1,
          0, false));
      return delegate(method, args);
    }

    private Object releaseRun(final Method method, final Object[] args, final MemorySegment run) throws Throwable {
      if (regionsReleased.get() >= regions.size() && !regions.isEmpty()) {
        callsAfterRegionsReleased.incrementAndGet();
      }
      final boolean fail = failReleaseRuns.getAndUpdate(n -> n > 0
          ? n - 1
          : 0) > 0;
      final Call call = locate(Kind.RELEASE_RUN, method.getName(), run, fail);
      calls.add(call);
      final RunObserver current = observer;
      if (current != null) {
        current.beforeRun(call);
      }
      if (fail) {
        return failure(method, releaseFailure, "injected release failure");
      }
      if (suppressOsEffect) {
        osEffectSuppressed.incrementAndGet();
        return success(method);
      }
      return delegate(method, args);
    }

    private Object recharge(final Method method, final Object[] args, final MemorySegment run) throws Throwable {
      final boolean fail = failRecharges.getAndUpdate(n -> n > 0
          ? n - 1
          : 0) > 0;
      calls.add(locate(Kind.RECHARGE, method.getName(), run, fail));
      if (fail) {
        return failure(method, rechargeFailure, "injected re-charge failure");
      }
      if (suppressOsEffect) {
        osEffectSuppressed.incrementAndGet();
        return success(method);
      }
      return delegate(method, args);
    }

    private Call locate(final Kind kind, final String name, final MemorySegment run, final boolean injected) {
      final long address = run.address();
      for (int classIdx = 0; classIdx < regions.size(); classIdx++) {
        final MemorySegment region = regions.get(classIdx);
        final long offset = address - region.address();
        if (offset >= 0 && offset + run.byteSize() <= region.byteSize()) {
          final long slotSize = FrameSlotAllocator.SIZE_CLASSES[classIdx];
          return new Call(seq.getAndIncrement(), kind, name, address, run.byteSize(), classIdx, offset / slotSize,
              Math.max(1L, run.byteSize() / slotSize), injected);
        }
      }
      return new Call(seq.getAndIncrement(), kind, name, address, run.byteSize(), -1, -1, 0, injected);
    }

    private Object failure(final Method method, final Failure mode, final String message) {
      if (mode == Failure.THROW) {
        throw new IllegalStateException(message);
      }
      final Class<?> result = method.getReturnType();
      if (result == boolean.class) {
        return false;
      }
      if (result == int.class) {
        return -1;
      }
      if (result == long.class) {
        return -1L;
      }
      if (result == void.class) {
        throw new IllegalStateException(message + " (the call returns void: it can only fail by throwing)");
      }
      return null;
    }

    private Object success(final Method method) {
      final Class<?> result = method.getReturnType();
      if (result == boolean.class) {
        return true;
      }
      if (result == int.class) {
        return 0;
      }
      if (result == long.class) {
        return 0L;
      }
      return null;
    }

    private Object delegate(final Method method, final Object[] args) throws Throwable {
      if (unmapped) {
        return method.getName().equals("commitFresh") || method.getName().equals("populate")
            ? (Object) false
            : success(method);
      }
      try {
        return method.invoke(real, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  private static boolean isPageSizeQuery(final String name) {
    final String lower = name.toLowerCase(Locale.ROOT);
    return lower.contains("page") && (lower.contains("size") || lower.contains("bytes"));
  }

  private static boolean isRechargeName(final String name) {
    final String lower = name.toLowerCase(Locale.ROOT);
    return lower.contains("recharge") || lower.contains("reuse") || lower.contains("recommit");
  }
}
