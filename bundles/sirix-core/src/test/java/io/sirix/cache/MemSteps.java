/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

/**
 * Which steps of the MEM chapter ({@code spec/MEM-memory-pool.md}) have landed. The spec-derived
 * tests of a step carry {@link MemPlanned} (or {@link MemPlannedProperty} for jqwik) and stay
 * disabled until the step's constant here is flipped to {@code true}: one edit per step, in this
 * one file, which enables every test of that step at once. A test cannot be switched on or off one
 * by one, and none can be weakened by editing its own annotation.
 *
 * <p>
 * Running the planned tests before their step lands is possible without editing anything:
 * {@code -Dsirix.mem.planned=run} enables all of them, so a reviewer can watch them fail now.
 *
 * <ul>
 * <li>Step 1: MEM-101 to MEM-109, MEM-401, MEM-402 ({@code FrameSlotAllocator},
 * {@link VirtualMemory} backends).</li>
 * <li>Step 2: MEM-201 to MEM-203 (one frame budget in the buffer manager).</li>
 * <li>Step 3: MEM-301, MEM-302 (compact-image pool as a reservation).</li>
 * </ul>
 */
final class MemSteps {

  /** System property that enables every planned test regardless of the constants below. */
  static final String FORCE_PROPERTY = "sirix.mem.planned";

  /** Whether MEM-101..109, MEM-401, MEM-402 have landed. */
  static final boolean STEP_1_LANDED = false;

  /** Whether MEM-201..203 have landed. */
  static final boolean STEP_2_LANDED = false;

  /** Whether MEM-301, MEM-302 have landed. */
  static final boolean STEP_3_LANDED = false;

  private MemSteps() {}

  static boolean isLanded(final int step) {
    return switch (step) {
      case 1 -> STEP_1_LANDED;
      case 2 -> STEP_2_LANDED;
      case 3 -> STEP_3_LANDED;
      default -> throw new IllegalArgumentException("the MEM chapter has steps 1, 2 and 3, not " + step);
    };
  }

  /** Whether {@code -Dsirix.mem.planned=run} forces the planned tests on. */
  static boolean forced() {
    return "run".equals(System.getProperty(FORCE_PROPERTY));
  }

  /** Whether the tests of {@code step} run in this JVM. */
  static boolean enabled(final int step) {
    return forced() || isLanded(step);
  }

  /** The reason text a disabled planned test reports. */
  static String disabledReason(final int step, final String[] requirements) {
    return String.join(", ", requirements) + " planned: enabled when MEM step " + step + " lands (flip MemSteps.STEP_"
        + step + "_LANDED; -D" + FORCE_PROPERTY + "=run runs it now)";
  }
}
