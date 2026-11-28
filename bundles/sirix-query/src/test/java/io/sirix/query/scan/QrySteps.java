/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

/**
 * Which landing steps of the QRY chapter ({@code spec/QRY-query-execution.md}, QRY-102's landing
 * order) have landed. The spec-derived tests of a step carry {@link QryPlanned} (or
 * {@link QryPlannedProperty} for jqwik) and stay disabled until the step's constant here is flipped
 * to {@code true}: one edit per step, in this one file, enables every test of that step at once. A
 * test cannot be switched on or off one by one, and none can be weakened by editing its own
 * annotation.
 *
 * <p>
 * Running the planned tests before their step lands needs no edit: {@code -Dsirix.qry.planned=run}
 * enables all of them, so a reviewer can watch them fail now. Every failure then names the missing
 * member, the requirement that needs it and the step that adds it (see {@link QrySeam}).
 *
 * <ul>
 * <li>Step 1: sirix compiles against both brackit snapshots (a {@code default} arm in each
 * exhaustive switch over {@code PredicateNode}). Its evidence is the compile itself; no test here
 * waits for it.</li>
 * <li>Step 2: brackit adds {@code PredicateNode.ArrayContainsPath} and the detection of QRY-101,
 * and sirix consumes that snapshot: QRY-101, QRY-102.</li>
 * <li>Step 3: the sirix route: QRY-201 to QRY-204, QRY-301 to QRY-304, QRY-401 to QRY-403, QRY-501
 * to QRY-503, QRY-504's composed-query routing check, QRY-601.</li>
 * </ul>
 *
 * <p>
 * QRY-103's negative table is not planned: no form it lists may ever be claimed, so it runs from
 * today and guards the brackit change of step 2 when it is published.
 */
public final class QrySteps {

  /** System property that enables every planned test regardless of the constants below. */
  public static final String FORCE_PROPERTY = "sirix.qry.planned";

  /** The step that adds {@code ArrayContainsPath} and its detection (brackit, consumed by sirix). */
  public static final int BRACKIT_DETECTION = 2;

  /** The step that adds the sirix route. */
  public static final int ROUTE = 3;

  /** Whether step 2 has landed: brackit's {@code ArrayContainsPath} and the QRY-101 detection. */
  static final boolean STEP_2_LANDED = false;

  /** Whether step 3 has landed: the column-served predicated-aggregate route. */
  static final boolean STEP_3_LANDED = false;

  private QrySteps() {
    throw new AssertionError("no instances");
  }

  static boolean isLanded(final int step) {
    return switch (step) {
      case BRACKIT_DETECTION -> STEP_2_LANDED;
      case ROUTE -> STEP_3_LANDED;
      default -> throw new IllegalArgumentException("QRY tests wait for step 2 or 3, not " + step);
    };
  }

  /** Whether {@code -Dsirix.qry.planned=run} forces the planned tests on. */
  static boolean forced() {
    return "run".equals(System.getProperty(FORCE_PROPERTY));
  }

  /** Whether the tests of {@code step} run in this JVM. */
  static boolean enabled(final int step) {
    return forced() || isLanded(step);
  }

  /** The reason text a disabled planned test reports. */
  static String disabledReason(final int step, final String[] requirements) {
    return String.join(", ", requirements) + " planned: enabled when QRY step " + step + " lands (flip QrySteps.STEP_"
        + step + "_LANDED; -D" + FORCE_PROPERTY + "=run runs it now)";
  }
}
