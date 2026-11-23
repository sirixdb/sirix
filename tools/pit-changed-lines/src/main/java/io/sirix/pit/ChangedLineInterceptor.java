package io.sirix.pit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.pitest.bytecode.analysis.ClassTree;
import org.pitest.mutationtest.build.InterceptorType;
import org.pitest.mutationtest.build.MutationInterceptor;
import org.pitest.mutationtest.engine.Mutater;
import org.pitest.mutationtest.engine.MutationDetails;

/**
 * Keeps the mutants on the lines a pull request changed and removes every other one (CONV-205).
 *
 * <p>
 * The lane's blocking criterion judges mutants on the PR's own lines, so those are the only mutants
 * worth analysing: an undetected mutant has to run every covering test to the end, and on PR 1353
 * that cost 1,098 of 1,132 test-case executions for mutants the verdict never reads.
 *
 * <p>
 * The declared {@link InterceptorType} decides when PIT applies this filter. PIT sorts a compound
 * interceptor pipeline by the ordinal of that type and runs its per-class mutant limit
 * ({@code CLASSLIMIT}) as a {@link InterceptorType#FILTER}, so
 * {@link InterceptorType#PRE_SCAN_FILTER} places this filter before any sampling — asserted against
 * the pinned PIT release by {@code ChangedLineInterceptorTest}, never assumed. A mutant it removes
 * is recorded in the {@link RestrictionCensus}, by identity and without a status.
 */
public final class ChangedLineInterceptor implements MutationInterceptor {

  private final ChangedLineMap changed;

  private final RestrictionCensus census;

  /**
   * @param changed the lines this pull request changed, per source file
   * @param census where the identities of removed mutants are recorded
   */
  public ChangedLineInterceptor(final ChangedLineMap changed, final RestrictionCensus census) {
    this.changed = changed;
    this.census = census;
  }

  /**
   * A filter that also applies in PIT's pre-scan, so a class with no mutant on a changed line needs
   * neither coverage nor analysis. Every ordinal below {@link InterceptorType#FILTER} runs before the
   * per-class limit; this one states the filter's actual role.
   */
  @Override
  public InterceptorType type() {
    return InterceptorType.PRE_SCAN_FILTER;
  }

  /**
   * Nothing to collect per class: every decision is made from the mutant's own line, so this filter
   * tolerates a caller that has no class tree to hand (its own tests, for one).
   */
  @Override
  public void begin(final @Nullable ClassTree clazz) {
    // Deliberately empty.
  }

  /** The mutater is unused for the same reason: a mutant's line and file decide its fate. */
  @Override
  public Collection<MutationDetails> intercept(final Collection<MutationDetails> mutations,
      final @Nullable Mutater mutater) {
    final List<MutationDetails> kept = new ArrayList<>(mutations.size());
    for (final MutationDetails mutation : mutations) {
      if (changed.contains(packageOf(mutation), mutation.getFilename(), mutation.getLineNumber())) {
        kept.add(mutation);
      } else {
        census.record(mutation);
      }
    }
    return kept;
  }

  @Override
  public void end() {
    // A class boundary: make the census durable, so a run killed by its deadline keeps it.
    census.flush();
  }

  private static String packageOf(final MutationDetails mutation) {
    return mutation.getClassName().getPackage().asJavaName();
  }
}
