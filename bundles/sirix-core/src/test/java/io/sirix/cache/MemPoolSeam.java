/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The one place where the MEM step-1 tests touch allocator members that do not exist yet. The tests
 * assert behavior; this class only <em>binds</em> them to the members the chapter requires, so that
 * the tests compile today and run the day the members exist.
 *
 * <p>
 * The chapter fixes behavior and the state it names, not method names. This class fixes one name
 * for each piece of state the tests must observe or inject, and fails a test with a message naming
 * the missing member, the requirement that needs it and the step that adds it (see
 * {@link #missing}). An implementer binds a different name by editing the string constants here;
 * the assertions of the tests stay as they are.
 *
 * <h2>Seam contract on {@link FrameSlotAllocator} (all package-private, step 1)</h2>
 *
 * <table>
 * <caption>Required members</caption>
 * <tr>
 * <th>Member</th>
 * <th>Meaning</th>
 * <th>Required by</th>
 * </tr>
 * <tr>
 * <td>constructor {@code (long budgetBytes, VirtualMemory vm)}, or a static factory taking the same
 * parameters</td>
 * <td>an allocator whose OS calls go to {@code vm} and whose OS page size is the one {@code vm}
 * reports</td>
 * <td>MEM-101, 102, 103, 104, 105, 107</td>
 * </tr>
 * <tr>
 * <td>{@code long mintedSlotCount(int classIdx)}</td>
 * <td>slots ever minted ({@code nextFreshIndex})</td>
 * <td>MEM-101, 105</td>
 * </tr>
 * <tr>
 * <td>{@code long returnedSlotCount(int classIdx)}</td>
 * <td>slots whose memory was given back and which are uncharged</td>
 * <td>MEM-101, 105, 402</td>
 * </tr>
 * <tr>
 * <td>{@code long inTransferSlotCount(int classIdx)}</td>
 * <td>slots claimed by a release batch</td>
 * <td>MEM-101</td>
 * </tr>
 * <tr>
 * <td>{@code long transferCount(int)}, {@code slotsReturnedCount(int)},
 * {@code bytesRefundedCount(int)}, {@code bytesRechargedCount(int)},
 * {@code failedReleaseCallCount(int)}, {@code lastReleaseErrorCode(int)}</td>
 * <td>the monotonic counters of MEM-402, per class</td>
 * <td>MEM-402</td>
 * </tr>
 * <tr>
 * <td>{@code static void setReleaseEverythingOnReleaseForTesting(boolean)}</td>
 * <td>the worst-case switch of MEM-104: every release of a slot returns it to the OS</td>
 * <td>MEM-104</td>
 * </tr>
 * </table>
 *
 * <p>
 * Everything else the tests use exists today: {@code allocate}, {@code release},
 * {@code allocateSlot}, {@code acquireVersion}, {@code validateVersion},
 * {@code getPhysicalMemoryBytes}, {@code liveSlotCount}, {@code recycledSlotCount},
 * {@code reserveScratchCommittedBytes}, {@code refundCommittedBytes}, {@code shutdown}.
 */
final class MemPoolSeam {

  static final String MINTED = "mintedSlotCount";
  static final String RETURNED = "returnedSlotCount";
  static final String IN_TRANSFER = "inTransferSlotCount";
  static final String TRANSFERS = "transferCount";
  static final String SLOTS_RETURNED = "slotsReturnedCount";
  static final String BYTES_REFUNDED = "bytesRefundedCount";
  static final String BYTES_RECHARGED = "bytesRechargedCount";
  static final String FAILED_RELEASE_CALLS = "failedReleaseCallCount";
  static final String LAST_RELEASE_ERROR = "lastReleaseErrorCode";
  static final String RELEASE_EVERYTHING_SWITCH = "setReleaseEverythingOnReleaseForTesting";

  /** One MiB. */
  static final long MIB = 1024L * 1024L;

  /** The transfer batch of MEM-103: 2 MiB, or one granule when a granule is larger. */
  static final long BATCH_BYTES = 2L * MIB;

  private MemPoolSeam() {}

  /** Raised for a required member that does not exist yet; carries the requirement and step. */
  static AssertionError missing(final String member, final String requirement, final int step) {
    return new AssertionError("MEM seam missing: " + member + " (needed by " + requirement + ", added by step " + step
        + "; see MemPoolSeam for the contract)");
  }

  /**
   * An allocator of {@code budgetBytes} whose OS calls go through {@code vm}.
   *
   * @throws AssertionError naming the missing seam when the allocator cannot be injected
   */
  static FrameSlotAllocator newAllocator(final long budgetBytes, final RecordingVirtualMemory vm) {
    for (final Constructor<?> constructor : FrameSlotAllocator.class.getDeclaredConstructors()) {
      final Class<?>[] types = constructor.getParameterTypes();
      if (types.length == 2 && types[0] == long.class && types[1] == VirtualMemory.class) {
        constructor.setAccessible(true);
        try {
          return (FrameSlotAllocator) constructor.newInstance(budgetBytes, vm.virtualMemory());
        } catch (final InvocationTargetException e) {
          throw rethrow(e.getCause());
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      }
    }
    for (final Method method : FrameSlotAllocator.class.getDeclaredMethods()) {
      final Class<?>[] types = method.getParameterTypes();
      if (Modifier.isStatic(method.getModifiers()) && method.getReturnType() == FrameSlotAllocator.class
          && types.length == 2 && types[0] == long.class && types[1] == VirtualMemory.class) {
        method.setAccessible(true);
        try {
          return (FrameSlotAllocator) method.invoke(null, budgetBytes, vm.virtualMemory());
        } catch (final InvocationTargetException e) {
          throw rethrow(e.getCause());
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      }
    }
    throw missing("FrameSlotAllocator(long budgetBytes, VirtualMemory vm)", "MEM-101 (injectable VirtualMemory)", 1);
  }

  /** Slots of {@code classIdx} ever minted ({@code nextFreshIndex}). */
  static long minted(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, MINTED, classIdx, "MEM-101, MEM-105");
  }

  /** Slots of {@code classIdx} whose memory was given back and which are uncharged. */
  static long returned(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, RETURNED, classIdx, "MEM-101, MEM-105, MEM-402");
  }

  /** Slots of {@code classIdx} claimed by a release batch. */
  static long inTransfer(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, IN_TRANSFER, classIdx, "MEM-101");
  }

  /** Transfers that returned at least one slot of {@code classIdx}. */
  static long transfers(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, TRANSFERS, classIdx, "MEM-402");
  }

  /** Slots of {@code classIdx} returned so far, monotonic. */
  static long slotsReturned(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, SLOTS_RETURNED, classIdx, "MEM-402");
  }

  /** Bytes refunded for {@code classIdx} so far, monotonic. */
  static long bytesRefunded(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, BYTES_REFUNDED, classIdx, "MEM-402");
  }

  /** Bytes re-charged for {@code classIdx} so far, monotonic. */
  static long bytesRecharged(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, BYTES_RECHARGED, classIdx, "MEM-402");
  }

  /** Failed release calls (and failed re-charge advice) for {@code classIdx} so far, monotonic. */
  static long failedReleaseCalls(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, FAILED_RELEASE_CALLS, classIdx, "MEM-107, MEM-402");
  }

  /** The error code of the last failed release call for {@code classIdx}. */
  static long lastReleaseError(final FrameSlotAllocator allocator, final int classIdx) {
    return counter(allocator, LAST_RELEASE_ERROR, classIdx, "MEM-402");
  }

  /** The sum of {@link #returned} over all seven classes. */
  static long returnedTotal(final FrameSlotAllocator allocator) {
    long sum = 0L;
    for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
      sum += returned(allocator, classIdx);
    }
    return sum;
  }

  /** Whether the worst-case switch of MEM-104 exists. */
  static boolean hasReleaseEverythingSwitch() {
    return findStatic(RELEASE_EVERYTHING_SWITCH, boolean.class) != null;
  }

  /**
   * Turn the worst-case switch of MEM-104 on or off: while on, every release of a slot returns it to
   * the OS.
   */
  static void releaseEverythingOnRelease(final boolean on) {
    final Method method = findStatic(RELEASE_EVERYTHING_SWITCH, boolean.class);
    if (method == null) {
      throw missing("FrameSlotAllocator." + RELEASE_EVERYTHING_SWITCH + "(boolean)", "MEM-104", 1);
    }
    try {
      method.invoke(null, on);
    } catch (final InvocationTargetException e) {
      throw rethrow(e.getCause());
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Installs a pressure listener that counts its invocations and records the byte argument of each,
   * whatever shape {@link FrameSlotAllocator.PressureListener} has (MEM-202 gives it the shortfall).
   * Closing restores the listener that was registered before.
   */
  static PressureProbe installPressureProbe() {
    return new PressureProbe();
  }

  /** A counting {@link FrameSlotAllocator.PressureListener}; closing restores the previous one. */
  static final class PressureProbe implements AutoCloseable {
    private final AtomicInteger fired = new AtomicInteger();
    private final AtomicLong lastShortfall = new AtomicLong(-1L);
    private final List<Long> shortfalls = new ArrayList<>();
    private final List<Object[]> invocations = new ArrayList<>();
    private final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    private volatile Runnable inside;

    private PressureProbe() {
      final InvocationHandler handler = (proxy, method, args) -> {
        if (method.getDeclaringClass() == Object.class) {
          return method.getName().equals("hashCode")
              ? System.identityHashCode(proxy)
              : method.getName().equals("equals")
                  ? proxy == args[0]
                  : "PressureProbe";
        }
        fired.incrementAndGet();
        synchronized (invocations) {
          invocations.add(args == null
              ? new Object[0]
              : args.clone());
        }
        if (args != null) {
          for (final Object argument : args) {
            if (argument instanceof Long shortfall) {
              lastShortfall.set(shortfall);
              synchronized (shortfalls) {
                shortfalls.add(shortfall);
              }
            }
          }
        }
        final Runnable action = inside;
        if (action != null) {
          action.run();
        }
        return null;
      };
      FrameSlotAllocator.setPressureListener((FrameSlotAllocator.PressureListener) Proxy.newProxyInstance(
          FrameSlotAllocator.PressureListener.class.getClassLoader(),
          new Class<?>[] {FrameSlotAllocator.PressureListener.class}, handler));
    }

    /** Run {@code action} inside every invocation, for example to release memory like a cache would. */
    void onPressure(final Runnable action) {
      this.inside = action;
    }

    int fired() {
      return fired.get();
    }

    /** The last shortfall argument the listener received, or {@code -1} when it had none. */
    long lastShortfall() {
      return lastShortfall.get();
    }

    /**
     * The arguments of every invocation, in order. MEM-103 and MEM-202 pass the shortfall kind (byte or
     * slot), its amount and the requesting frame class; the chapter fixes no parameter types, so a
     * listener decodes them with {@link PressureRequest}.
     */
    List<Object[]> invocations() {
      synchronized (invocations) {
        return new ArrayList<>(invocations);
      }
    }

    /** Every shortfall argument received, in order. */
    List<Long> shortfalls() {
      synchronized (shortfalls) {
        return new ArrayList<>(shortfalls);
      }
    }

    @Override
    public void close() {
      FrameSlotAllocator.setPressureListener(previous);
    }
  }

  /**
   * A pressure request decoded by shape from the arguments of one listener invocation: an
   * {@code int}/{@link Integer} in the class range is the requesting frame class, a {@code long} is
   * the amount (bytes for a byte shortfall, slots for a slot shortfall), and any other argument whose
   * {@code toString()} contains {@code SLOT} (case-insensitively) marks a slot shortfall. A request
   * without a kind argument is a byte request.
   */
  record PressureRequest(boolean slotShortfall, long amount, int requestingClass) {

    /** Decode {@code args}; the class is {@code -1} when none was passed. */
    static PressureRequest decode(final Object[] args) {
      boolean slot = false;
      long amount = -1L;
      int requestingClass = -1;
      for (final Object argument : args) {
        if (argument instanceof Long value) {
          amount = value;
        } else if (argument instanceof Integer value && value >= 0 && value < FrameSlotAllocator.SIZE_CLASSES.length) {
          requestingClass = value;
        } else if (argument != null && !(argument instanceof Number) && !(argument instanceof Boolean)
            && argument.toString().toUpperCase(Locale.ROOT).contains("SLOT")) {
          slot = true;
        }
      }
      return new PressureRequest(slot, amount, requestingClass);
    }
  }

  private static long counter(final FrameSlotAllocator allocator, final String name, final int classIdx,
      final String requirement) {
    final Method method = find(name, int.class);
    if (method == null) {
      throw missing("FrameSlotAllocator." + name + "(int classIdx)", requirement, 1);
    }
    try {
      return ((Number) method.invoke(allocator, classIdx)).longValue();
    } catch (final InvocationTargetException e) {
      throw rethrow(e.getCause());
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static Method find(final String name, final Class<?>... types) {
    try {
      final Method method = FrameSlotAllocator.class.getDeclaredMethod(name, types);
      method.setAccessible(true);
      return method;
    } catch (final NoSuchMethodException e) {
      return null;
    }
  }

  private static Method findStatic(final String name, final Class<?>... types) {
    final Method method = find(name, types);
    return method != null && Modifier.isStatic(method.getModifiers())
        ? method
        : null;
  }

  private static RuntimeException rethrow(final Throwable failure) {
    if (failure instanceof RuntimeException runtime) {
      return runtime;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    throw new AssertionError(failure);
  }
}
