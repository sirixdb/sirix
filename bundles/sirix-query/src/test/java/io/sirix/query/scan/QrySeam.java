/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.brackit.query.compiler.optimizer.PredicateNode;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * The one place where the QRY tests touch members that do not exist yet. The tests assert behavior;
 * this class only <em>binds</em> them to the members the chapter requires, so that the tests
 * compile today and run the day the members exist.
 *
 * <p>
 * The chapter fixes behavior and the state it names, not member names. This class fixes one name
 * for each piece of state the tests observe or switch, and fails a test with a message naming the
 * missing member, the requirement that needs it and the step that adds it (see {@link #missing}).
 * An implementer binds a different name by editing the string constants and tables here; the
 * assertions of the tests stay as they are.
 *
 * <h2>Seam contract on {@link SirixVectorizedExecutor} (static, step 3)</h2>
 *
 * <table>
 * <caption>Required members</caption>
 * <tr>
 * <th>Member</th>
 * <th>Meaning</th>
 * <th>Required by</th>
 * </tr>
 * <tr>
 * <td>{@code long predicatedRegionPagesServed()}</td>
 * <td>pages whose contribution this route computed from their regions</td>
 * <td>QRY-503, every route test</td>
 * </tr>
 * <tr>
 * <td>{@code long predicatedRegionPagesDeclined(PredicatedRegionDecline reason)}</td>
 * <td>pages handed to the record path, by reason; one enum value per decline condition of QRY-203,
 * QRY-301, QRY-302 and QRY-303 (and the fragment-merge decline of QRY-402)</td>
 * <td>QRY-503, QRY-203, QRY-302, QRY-304, QRY-501</td>
 * </tr>
 * <tr>
 * <td>{@code long predicatedRegionSeamRecords()}</td>
 * <td>seam records handed to the record path</td>
 * <td>QRY-503, QRY-501</td>
 * </tr>
 * <tr>
 * <td>{@code long predicatedRegionRefusals(PredicatedRegionRefusal reason)}</td>
 * <td>query refusals, by reason</td>
 * <td>QRY-503, QRY-204, QRY-304</td>
 * </tr>
 * <tr>
 * <td>{@code long predicateFullRecordPageReads()}</td>
 * <td>every {@code getRecordPage} read the executor makes while it evaluates a claimed predicated
 * count or aggregate, from this route's record path or from
 * {@code parallelGenericPredicateAggregate}</td>
 * <td>QRY-503, QRY-501</td>
 * </tr>
 * <tr>
 * <td>{@code long predicatedRegionColumnReads()}</td>
 * <td>region (column) reads of this route other than the name-key column</td>
 * <td>QRY-501 item 3 (QRY-503 does not list it; a page without the anchor field must read
 * none)</td>
 * </tr>
 * <tr>
 * <td>{@code void resetPredicatedRegionCounters()}</td>
 * <td>the reset hook for tests: every counter above to zero</td>
 * <td>QRY-503</td>
 * </tr>
 * <tr>
 * <td>{@code static volatile boolean PREDICATED_REGION_ENABLED}, initialised from
 * {@code -Dsirix.scan.predicatedRegion}</td>
 * <td>the route's own kill switch, independent of {@code sirix.scan.arrayContainsColumnar}</td>
 * <td>QRY-601</td>
 * </tr>
 * <tr>
 * <td>{@code static volatile boolean} {@code QRY_SEAM_SKIP_FLAG_SCOPE},
 * {@code QRY_SEAM_SKIP_MEMBER_SCOPE}, {@code QRY_SEAM_SKIP_VALUE_SCOPE}</td>
 * <td>test-only mutation seams, off by default as in {@code ArrayContainsScopeDifferentialTest}:
 * each bypasses one scope proof of QRY-301, that a value counted, tested or added belongs to the
 * flag path, the member path or the {@code N} path. (The proof that no other structure reaches the
 * path has no seam: brackit's deref does not reach through an array of objects, so bypassing it
 * cannot change an answer, and a seam that cannot bite proves nothing.)</td>
 * <td>QRY-301</td>
 * </tr>
 * </table>
 *
 * <h2>Seam contract on brackit (step 2)</h2>
 *
 * <p>
 * {@code PredicateNode.ArrayContainsPath(List<String> path, String value)}, a record with accessors
 * {@code path()} and {@code value()} (QRY-102, decision D1).
 */
public final class QrySeam {

  static final String SERVED = "predicatedRegionPagesServed";
  static final String DECLINED = "predicatedRegionPagesDeclined";
  static final String DECLINE_ENUM = "io.sirix.query.scan.SirixVectorizedExecutor$PredicatedRegionDecline";
  static final String SEAM_RECORDS = "predicatedRegionSeamRecords";
  static final String REFUSED = "predicatedRegionRefusals";
  static final String REFUSAL_ENUM = "io.sirix.query.scan.SirixVectorizedExecutor$PredicatedRegionRefusal";
  static final String RECORD_PAGE_READS = "predicateFullRecordPageReads";
  static final String COLUMN_READS = "predicatedRegionColumnReads";
  static final String RESET = "resetPredicatedRegionCounters";
  static final String SWITCH_FIELD = "PREDICATED_REGION_ENABLED";

  /** The route's own system property (QRY-601). */
  public static final String SWITCH_PROPERTY = "sirix.scan.predicatedRegion";

  static final String ARRAY_CONTAINS_PATH = "io.brackit.query.compiler.optimizer.PredicateNode$ArrayContainsPath";

  /**
   * Why a page was handed to the record path; the implementation's constant names are bound below.
   */
  public enum Decline {
    /** QRY-203: an in-scope value of a type the route does not model on that page. */
    VALUE_TYPE,
    /** QRY-301: the page's values cannot be proven to belong to the queried path. */
    SCOPE,
    /**
     * QRY-302 items 1 and 2: a flag bit, element or {@code N} value cannot be attributed to its record.
     */
    ATTRIBUTION,
    /** QRY-302 item 3: a record on the page carries a used field more than once. */
    DUPLICATE_FIELD,
    /** QRY-302 item 4: the element-count certificate of a member path does not hold. */
    ELEMENT_CERTIFICATE,
    /** QRY-303: a seam the page cannot settle. */
    SEAM,
    /** QRY-402: merged fragment columns that cannot be proven equal to the reconstructed page's. */
    FRAGMENT_MERGE
  }

  /** Why a whole query was refused; the implementation's constant names are bound below. */
  public enum Refusal {
    /** QRY-204 N2: a double contributes to a {@code sum}. */
    DOUBLE_SUM,
    /** QRY-304: claimed but not servable, an open write transaction ({@code wtx != null}). */
    WRITE_TRANSACTION,
    /** QRY-301: a path that is ambiguous, has no summary, or is reached through another structure. */
    SCOPE,
    /**
     * QRY-203 / QRY-304: a value or leaf the record path cannot evaluate with interpreter semantics.
     */
    RECORD_PATH
  }

  /** A QRY-301 scope proof that a mutation seam can bypass. */
  public enum ScopeProof {
    FLAG("QRY_SEAM_SKIP_FLAG_SCOPE"), MEMBER("QRY_SEAM_SKIP_MEMBER_SCOPE"), VALUE("QRY_SEAM_SKIP_VALUE_SCOPE");

    private final String field;

    ScopeProof(final String field) {
      this.field = field;
    }
  }

  /** Test reason to the implementation's enum constant name. Rebind here, not in the tests. */
  private static final Map<Decline, String> DECLINE_NAMES = new EnumMap<>(Decline.class);
  private static final Map<Refusal, String> REFUSAL_NAMES = new EnumMap<>(Refusal.class);

  static {
    DECLINE_NAMES.put(Decline.VALUE_TYPE, "VALUE_TYPE");
    DECLINE_NAMES.put(Decline.SCOPE, "SCOPE");
    DECLINE_NAMES.put(Decline.ATTRIBUTION, "ATTRIBUTION");
    DECLINE_NAMES.put(Decline.DUPLICATE_FIELD, "DUPLICATE_FIELD");
    DECLINE_NAMES.put(Decline.ELEMENT_CERTIFICATE, "ELEMENT_CERTIFICATE");
    DECLINE_NAMES.put(Decline.SEAM, "SEAM");
    DECLINE_NAMES.put(Decline.FRAGMENT_MERGE, "FRAGMENT_MERGE");
    REFUSAL_NAMES.put(Refusal.DOUBLE_SUM, "DOUBLE_SUM");
    REFUSAL_NAMES.put(Refusal.WRITE_TRANSACTION, "WRITE_TRANSACTION");
    REFUSAL_NAMES.put(Refusal.SCOPE, "SCOPE");
    REFUSAL_NAMES.put(Refusal.RECORD_PATH, "RECORD_PATH");
  }

  private QrySeam() {
    throw new AssertionError("no instances");
  }

  /** Raised for a required member that does not exist yet; carries the requirement and step. */
  static AssertionError missing(final String member, final String requirement, final int step) {
    return new AssertionError("QRY seam missing: " + member + " (needed by " + requirement + ", added by step " + step
        + "; see QrySeam for the contract)");
  }

  // -----------------------------------------------------------------------------------------------
  // Counters (QRY-503)
  // -----------------------------------------------------------------------------------------------

  /** One snapshot of the route's counters; {@link #outcome()} classifies a query from a delta. */
  public record Counters(long served, long declined, Map<Decline, Long> declinedBy, long seamRecords, long refused,
      Map<Refusal, Long> refusedBy, long recordPageReads, long columnReads) {

    public long declined(final Decline reason) {
      return declinedBy.getOrDefault(reason, 0L);
    }

    public long refused(final Refusal reason) {
      return refusedBy.getOrDefault(reason, 0L);
    }

    /** Pages handled by the route, served or declined. */
    public long handled() {
      return served + declined;
    }

    /** What happened to the query, by QRY-304's three outcomes. */
    public RouteOutcome outcome() {
      if (refused > 0) {
        return RouteOutcome.REFUSED;
      }
      if (declined > 0) {
        return RouteOutcome.PAGE_DECLINED;
      }
      if (served > 0) {
        return RouteOutcome.SERVED;
      }
      return RouteOutcome.NOT_REACHED;
    }

    @Override
    public String toString() {
      return "served=" + served + ", declined=" + declined + " " + declinedBy + ", seamRecords=" + seamRecords
          + ", refused=" + refused + " " + refusedBy + ", recordPageReads=" + recordPageReads + ", columnReads="
          + columnReads;
    }
  }

  /** QRY-304's outcomes, plus the one every route test must rule out: the route was never reached. */
  public enum RouteOutcome {
    /** Every handled page was served from its regions. */
    SERVED,
    /** At least one page was declined and decided by the record path; nothing refused. */
    PAGE_DECLINED,
    /** The query was refused and the interpreter evaluated it. */
    REFUSED,
    /** No counter moved: the query never reached the route. */
    NOT_REACHED
  }

  /** Zeroes every route counter. */
  public static void reset() {
    invokeStatic(RESET, "QRY-503");
  }

  /** The route's counters now. */
  public static Counters counters() {
    final Map<Decline, Long> declinedBy = new EnumMap<>(Decline.class);
    long declined = 0;
    final Class<?> declineEnum = enumClass(DECLINE_ENUM, "QRY-503");
    for (final Object constant : declineEnum.getEnumConstants()) {
      final long n = longStatic(DECLINED, "QRY-503", declineEnum, constant);
      declined += n;
      final Decline reason = testReason(DECLINE_NAMES, ((Enum<?>) constant).name());
      if (reason != null) {
        declinedBy.merge(reason, n, Long::sum);
      }
    }
    final Map<Refusal, Long> refusedBy = new EnumMap<>(Refusal.class);
    long refused = 0;
    final Class<?> refusalEnum = enumClass(REFUSAL_ENUM, "QRY-503");
    for (final Object constant : refusalEnum.getEnumConstants()) {
      final long n = longStatic(REFUSED, "QRY-503", refusalEnum, constant);
      refused += n;
      final Refusal reason = testReason(REFUSAL_NAMES, ((Enum<?>) constant).name());
      if (reason != null) {
        refusedBy.merge(reason, n, Long::sum);
      }
    }
    return new Counters(longStatic(SERVED, "QRY-503", null, null), declined, declinedBy,
        longStatic(SEAM_RECORDS, "QRY-503", null, null), refused, refusedBy,
        longStatic(RECORD_PAGE_READS, "QRY-503", null, null), longStatic(COLUMN_READS, "QRY-501", null, null));
  }

  /** {@link #counters()} after {@code action}, which runs on freshly reset counters. */
  public static Counters around(final ThrowingRunnable action) throws Exception {
    reset();
    action.run();
    return counters();
  }

  /** An action that may throw. */
  @FunctionalInterface
  public interface ThrowingRunnable {
    void run() throws Exception;
  }

  /** Requires that every bound decline/refusal reason exists in the implementation's enum. */
  public static void requireReasonsBound() {
    final Class<?> declineEnum = enumClass(DECLINE_ENUM, "QRY-503");
    for (final Map.Entry<Decline, String> e : DECLINE_NAMES.entrySet()) {
      requireConstant(declineEnum, e.getValue(), "decline " + e.getKey());
    }
    final Class<?> refusalEnum = enumClass(REFUSAL_ENUM, "QRY-503");
    for (final Map.Entry<Refusal, String> e : REFUSAL_NAMES.entrySet()) {
      requireConstant(refusalEnum, e.getValue(), "refusal " + e.getKey());
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Switch (QRY-601) and mutation seams (QRY-301)
  // -----------------------------------------------------------------------------------------------

  /** Turns the route on or off with no rebuild; returns the previous value. */
  public static boolean setRouteEnabled(final boolean enabled) {
    final Field field = staticField(SWITCH_FIELD, "QRY-601");
    try {
      final boolean previous = field.getBoolean(null);
      field.setBoolean(null, enabled);
      return previous;
    } catch (final IllegalAccessException e) {
      throw new AssertionError("QRY seam " + SWITCH_FIELD + " is not writable", e);
    }
  }

  /** Bypasses (or restores) one QRY-301 scope proof. */
  public static void setScopeProofBypassed(final ScopeProof proof, final boolean bypassed) {
    final Field field = staticField(proof.field, "QRY-301");
    try {
      field.setBoolean(null, bypassed);
    } catch (final IllegalAccessException e) {
      throw new AssertionError("QRY seam " + proof.field + " is not writable", e);
    }
  }

  /**
   * Restores every QRY-301 seam to off, tolerating a route that does not exist yet: tear-down must
   * not mask the test's own failure.
   */
  public static void resetScopeProofsQuietly() {
    for (final ScopeProof proof : ScopeProof.values()) {
      try {
        final Field field = SirixVectorizedExecutor.class.getDeclaredField(proof.field);
        field.setAccessible(true);
        field.setBoolean(null, false);
      } catch (final NoSuchFieldException | IllegalAccessException | RuntimeException ignored) {
        // Not there yet: nothing to restore.
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // brackit (step 2)
  // -----------------------------------------------------------------------------------------------

  /** {@code new PredicateNode.ArrayContainsPath(path, value)}. */
  public static PredicateNode arrayContainsPath(final List<String> path, final String value) {
    final Class<?> type = arrayContainsPathClass();
    try {
      final Constructor<?> constructor = type.getDeclaredConstructor(List.class, String.class);
      return (PredicateNode) constructor.newInstance(path, value);
    } catch (final InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new AssertionError(e.getCause());
    } catch (final NoSuchMethodException e) {
      throw missing("constructor ArrayContainsPath(List<String>, String)", "QRY-102", QrySteps.BRACKIT_DETECTION);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("QRY seam ArrayContainsPath is not constructible", e);
    }
  }

  /** Whether {@code node} is an {@code ArrayContainsPath} leaf. */
  public static boolean isArrayContainsPath(final PredicateNode node) {
    return node != null && node.getClass().getName().equals(ARRAY_CONTAINS_PATH);
  }

  /** {@code ArrayContainsPath.path()}. */
  @SuppressWarnings("unchecked")
  public static List<String> pathOf(final PredicateNode node) {
    return (List<String>) accessor(node, "path");
  }

  /** {@code ArrayContainsPath.value()}. */
  public static String valueOf(final PredicateNode node) {
    return (String) accessor(node, "value");
  }

  private static Object accessor(final PredicateNode node, final String name) {
    final Class<?> type = arrayContainsPathClass();
    try {
      final Method method = type.getMethod(name);
      return method.invoke(type.cast(node));
    } catch (final NoSuchMethodException e) {
      throw missing("ArrayContainsPath." + name + "()", "QRY-102", QrySteps.BRACKIT_DETECTION);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("QRY seam ArrayContainsPath." + name + "() failed", e);
    }
  }

  private static Class<?> arrayContainsPathClass() {
    try {
      return Class.forName(ARRAY_CONTAINS_PATH);
    } catch (final ClassNotFoundException e) {
      throw missing("PredicateNode.ArrayContainsPath", "QRY-102", QrySteps.BRACKIT_DETECTION);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // reflection
  // -----------------------------------------------------------------------------------------------

  private static <R extends Enum<R>> R testReason(final Map<R, String> names, final String implementationName) {
    for (final Map.Entry<R, String> e : names.entrySet()) {
      if (e.getValue().equals(implementationName)) {
        return e.getKey();
      }
    }
    return null;
  }

  private static void requireConstant(final Class<?> enumType, final String name, final String what) {
    for (final Object constant : enumType.getEnumConstants()) {
      if (((Enum<?>) constant).name().equals(name)) {
        return;
      }
    }
    throw missing(enumType.getSimpleName() + "." + name + " (" + what + ")", "QRY-503", QrySteps.ROUTE);
  }

  private static Class<?> enumClass(final String name, final String requirement) {
    try {
      final Class<?> type = Class.forName(name);
      if (!type.isEnum()) {
        throw new AssertionError("QRY seam " + name + " is not an enum");
      }
      return type;
    } catch (final ClassNotFoundException e) {
      throw missing(name, requirement, QrySteps.ROUTE);
    }
  }

  private static long longStatic(final String name, final String requirement, final Class<?> parameter,
      final Object argument) {
    try {
      final Method method = parameter == null
          ? SirixVectorizedExecutor.class.getDeclaredMethod(name)
          : SirixVectorizedExecutor.class.getDeclaredMethod(name, parameter);
      requireStatic(method.getModifiers(), name);
      method.setAccessible(true);
      return (long) (parameter == null
          ? method.invoke(null)
          : method.invoke(null, argument));
    } catch (final NoSuchMethodException e) {
      throw missing("SirixVectorizedExecutor." + name, requirement, QrySteps.ROUTE);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("QRY seam " + name + " failed", e);
    }
  }

  private static void invokeStatic(final String name, final String requirement) {
    try {
      final Method method = SirixVectorizedExecutor.class.getDeclaredMethod(name);
      requireStatic(method.getModifiers(), name);
      method.setAccessible(true);
      method.invoke(null);
    } catch (final NoSuchMethodException e) {
      throw missing("SirixVectorizedExecutor." + name + "()", requirement, QrySteps.ROUTE);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError("QRY seam " + name + " failed", e);
    }
  }

  private static Field staticField(final String name, final String requirement) {
    try {
      final Field field = SirixVectorizedExecutor.class.getDeclaredField(name);
      requireStatic(field.getModifiers(), name);
      if (field.getType() != boolean.class) {
        throw new AssertionError("QRY seam " + name + " must be a static boolean");
      }
      field.setAccessible(true);
      return field;
    } catch (final NoSuchFieldException e) {
      throw missing("SirixVectorizedExecutor." + name, requirement, QrySteps.ROUTE);
    }
  }

  private static void requireStatic(final int modifiers, final String name) {
    if (!Modifier.isStatic(modifiers)) {
      throw new AssertionError("QRY seam " + name + " must be static");
    }
  }
}
