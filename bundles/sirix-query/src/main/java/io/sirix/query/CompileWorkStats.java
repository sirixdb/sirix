package io.sirix.query;

import java.util.concurrent.atomic.AtomicLong;

/** Compile-phase invocations for deterministic work budgets; disabled in production. */
public final class CompileWorkStats {
  public static final boolean ENABLED = Boolean.getBoolean("sirix.query.compileWorkDiag");
  private static final AtomicLong PARSES = new AtomicLong();
  private static final AtomicLong ANALYSES = new AtomicLong();
  private static final AtomicLong OPTIMIZATIONS = new AtomicLong();
  private static final AtomicLong TRANSLATIONS = new AtomicLong();
  private static final AtomicLong TRANSLATED_EXPRESSIONS = new AtomicLong();

  private CompileWorkStats() {
    throw new AssertionError("no instances");
  }

  static void parsed() {
    if (ENABLED) PARSES.incrementAndGet();
  }

  static void analysed() {
    if (ENABLED) ANALYSES.incrementAndGet();
  }

  static void optimized() {
    if (ENABLED) OPTIMIZATIONS.incrementAndGet();
  }

  static void translated() {
    if (ENABLED) TRANSLATIONS.incrementAndGet();
  }

  public static long parses() {
    return PARSES.get();
  }

  public static long analyses() {
    return ANALYSES.get();
  }

  public static long optimizations() {
    return OPTIMIZATIONS.get();
  }

  public static long translations() {
    return TRANSLATIONS.get();
  }

  /** Actual recursive expression-dispatch work, rather than an AST-size estimate. */
  public static void translatedExpression() {
    if (ENABLED) TRANSLATED_EXPRESSIONS.incrementAndGet();
  }

  public static long translatedExpressions() {
    return TRANSLATED_EXPRESSIONS.get();
  }
}
