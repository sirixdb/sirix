package io.sirix.pit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.pit.ChangedLineMap.ChangedLineMapException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pitest.bytecode.analysis.ClassTree;
import org.pitest.classinfo.ClassName;
import org.pitest.mutationtest.build.CompoundMutationInterceptor;
import org.pitest.mutationtest.build.InterceptorParameters;
import org.pitest.mutationtest.build.InterceptorType;
import org.pitest.mutationtest.build.MutationInterceptor;
import org.pitest.mutationtest.engine.Location;
import org.pitest.mutationtest.engine.Mutater;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.mutationtest.filter.LimitNumberOfMutationsPerClassFilterFactory;
import org.pitest.plugin.Feature;
import org.pitest.plugin.FeatureParser;
import org.pitest.plugin.FeatureSetting;
import org.pitest.plugin.ToggleStatus;

/**
 * CONV-205's restriction: the lane analyses the mutants on the pull request's own lines, removes
 * every other one before any sampling, and records what it removed by identity and without a
 * status.
 */
final class ChangedLineInterceptorTest {

  private static final String MUTATOR =
      "org.pitest.mutationtest.engine.gregor.mutators.ReturnValsMutator";

  @TempDir
  private Path directory;

  private static MutationDetails mutant(final String type, final String file, final int line,
      final int index) {
    return new MutationDetails(new MutationIdentifier(
        Location.location(ClassName.fromString(type), "commit", "()V"), List.of(index), MUTATOR),
        file, "replaced return value with null", line, 0);
  }

  private Path map(final String... records) throws IOException {
    final Path path = directory.resolve("changed-lines.tsv");
    Files.writeString(path, ChangedLineMap.HEADER + '\n' + String.join("\n", records) + '\n');
    return path;
  }

  private Path census() {
    return directory.resolve(ChangedLineInterceptorFactory.CENSUS_FILE);
  }

  private static Collection<MutationDetails> run(final MutationInterceptor interceptor,
      final Collection<MutationDetails> mutations) {
    // PIT hands a class tree and a mutater to a real pipeline; this filter reads neither, and
    // both interceptors under test here state that they tolerate their absence.
    interceptor.begin(null);
    final Collection<MutationDetails> kept = interceptor.intercept(mutations, null);
    interceptor.end();
    return kept;
  }

  private static List<String> lines(final Collection<MutationDetails> mutations) {
    final List<String> described = new ArrayList<>();
    for (final MutationDetails mutation : mutations) {
      described.add(mutation.getClassName().asJavaName() + ':' + mutation.getLineNumber());
    }
    return described;
  }

  @Test
  void keepsTheMutantsOnChangedLinesAndRemovesEveryOther() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24 36"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));

    final Collection<MutationDetails> kept = run(interceptor, List.of(
        mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 24, 1),
        mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 30, 2),
        mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 3),
        mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 99, 4)));

    assertEquals(List.of("io.sirix.api.NodeTrx:24", "io.sirix.api.NodeTrx:36"), lines(kept));
  }

  @Test
  void removesEveryMutantOfASourceTheMapDoesNotList() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));

    final Collection<MutationDetails> kept = run(interceptor, List.of(
        mutant("io.sirix.api.Other", "Other.java", 24, 1),
        mutant("io.sirix.api.xml.NodeTrx", "NodeTrx.java", 24, 2)));

    assertTrue(kept.isEmpty(), "an unlisted source has no changed line, so no mutant in scope");
  }

  @Test
  void attributesNestedAndExtraTopLevelClassesToTheirSourceFile() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));

    final Collection<MutationDetails> kept = run(interceptor, List.of(
        mutant("io.sirix.api.NodeTrx$1", "NodeTrx.java", 24, 1),
        mutant("io.sirix.api.Helper", "NodeTrx.java", 24, 2)));

    assertEquals(List.of("io.sirix.api.NodeTrx$1:24", "io.sirix.api.Helper:24"), lines(kept),
        "a class compiled from a changed source is judged by that source's lines");
  }

  @Test
  void censusesEveryRemovedMutantByIdentityWithItsLine() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));

    run(interceptor, List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 24, 1),
        mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 7)));

    assertEquals(List.of("{\"class\":\"io.sirix.api.NodeTrx\",\"method\":\"commit\","
        + "\"descriptor\":\"()V\",\"mutator\":\"" + MUTATOR + "\",\"indexes\":[7],"
        + "\"description\":\"replaced return value with null\","
        + "\"sourceFile\":\"NodeTrx.java\",\"lineNumber\":36}"),
        Files.readAllLines(census()), "the kept mutant is absent and no status is recorded");
  }

  @Test
  void censusIsReadableAfterEachClassWithoutAnEndOfRunHook() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));

    run(interceptor, List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 1)));
    assertEquals(1, Files.readAllLines(census()).size(), "the first class is already durable");

    run(interceptor, List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 37, 2)));
    assertEquals(2, Files.readAllLines(census()).size(), "the second class appends to it");
  }

  @Test
  void censusesEachIdentityOnlyOnceAcrossRepeatedPasses() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));
    final List<MutationDetails> mutations =
        List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 1));

    run(interceptor, mutations);
    run(interceptor, mutations);

    assertEquals(1, Files.readAllLines(census()).size(),
        "PIT's pre-scan sees a class twice; the census counts the mutant once");
  }

  @Test
  void thePinnedReleaseReadsOnlyAFeaturesFirstParameter() {
    // Why the census has a derived location and not a parameter of its own: 1.25.5 splits a
    // feature string on ']' and then on '[', so every later parameter's key keeps the comma that
    // separated them and no lookup finds it - silently, with no error. A release that fixes this
    // must fail here, so the decision is revisited rather than inherited.
    final FeatureSetting parsed = new FeatureParser()
        .parseFeatures(List.of("+CHANGEDLINES(file[/map.tsv],census[/census.jsonl])")).get(0);

    assertEquals(Optional.of("/map.tsv"), parsed.getString("file"));
    assertEquals(Optional.empty(), parsed.getString("census"),
        "a second feature parameter is not readable in this release");
  }

  @Test
  void escapesCensusTextThatWouldBreakOneRecordPerLine() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));
    final MutationDetails awkward = new MutationDetails(new MutationIdentifier(
        Location.location(ClassName.fromString("io.sirix.api.NodeTrx"), "commit", "()V"),
        List.of(1), MUTATOR), "NodeTrx.java", "replaced \"x\"\nwith \\null", 36, 0);

    run(interceptor, List.of(awkward));

    final List<String> records = Files.readAllLines(census());
    assertEquals(1, records.size(), "an embedded newline must not split the record");
    assertTrue(records.get(0).contains("\\\"x\\\"\\nwith \\\\null"), records.get(0));
  }

  @Test
  void escapesAControlCharacterAsAJsonEscape() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor interceptor =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));
    final MutationDetails awkward = new MutationDetails(new MutationIdentifier(
        Location.location(ClassName.fromString("io.sirix.api.NodeTrx"), "commit", "()V"),
        List.of(1), MUTATOR), "NodeTrx.java", "replaced \u0001 and \u001f", 36, 0);

    run(interceptor, List.of(awkward));

    assertTrue(Files.readAllLines(census()).get(0).contains("replaced \\u0001 and \\u001f"),
        Files.readAllLines(census()).get(0));
  }

  @Test
  void runsBeforeThePerClassLimitOfThePinnedRelease() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24 36"));
    final MutationInterceptor restriction =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));
    final MutationInterceptor limit = new LimitNumberOfMutationsPerClassFilterFactory()
        .createInterceptor(new InterceptorParameters(
            new FeatureSetting("CLASSLIMIT", ToggleStatus.ACTIVATE,
                Map.of("limit", List.of("1"))),
            null, null, null, null, null, null));

    assertEquals(InterceptorType.FILTER, limit.type(),
        "the release still runs its per-class limit as a FILTER");
    assertTrue(restriction.type().ordinal() < limit.type().ordinal(),
        "PIT sorts a compound pipeline by interceptor type, so the restriction must sort first");

    // Listed limit-first on purpose: the compound must reorder it behind the restriction.
    final Collection<MutationDetails> kept =
        run(new CompoundMutationInterceptor(List.of(limit, restriction)),
            List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 99, 1),
                mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 24, 2),
                mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 98, 3)));

    assertEquals(List.of("io.sirix.api.NodeTrx:24"), lines(kept),
        "the limit sampled the changed-line mutant, not an off-diff one");
  }

  @Test
  void noLaterFilterEverSeesAnOffDiffMutant() throws IOException {
    final ChangedLineMap changed = ChangedLineMap.read(map("io.sirix.api\tNodeTrx.java\t24"));
    final MutationInterceptor restriction =
        new ChangedLineInterceptor(changed, RestrictionCensus.at(census()));
    final RecordingFilter later = new RecordingFilter();

    run(new CompoundMutationInterceptor(List.of(later, restriction)),
        List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 24, 1),
            mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 2)));

    assertEquals(List.of("io.sirix.api.NodeTrx:24"), lines(later.seen),
        "every FILTER-typed interceptor, CLASSLIMIT included, runs after the restriction");
  }

  @Test
  void theFeatureNeedsAReadableMap() throws IOException {
    final ChangedLineInterceptorFactory factory = new ChangedLineInterceptorFactory();
    final Path missing = directory.resolve("absent.tsv");

    assertThrows(ChangedLineMapException.class, () -> factory.createInterceptor(parameters(null)),
        "no file parameter");
    assertThrows(ChangedLineMapException.class, () -> factory.createInterceptor(parameters("  ")),
        "blank file parameter");
    assertThrows(ChangedLineMapException.class,
        () -> factory.createInterceptor(parameters(missing.toString())), "absent map");
  }

  @Test
  void theFeatureDeclaresItsNameAndParameters() {
    final ChangedLineInterceptorFactory factory = new ChangedLineInterceptorFactory();

    // PIT normalises a feature name, so compare against what it resolves `+CHANGEDLINES` to
    // rather than against the spelling the Gradle configuration writes.
    assertEquals(Feature.named("CHANGEDLINES").name(), factory.provides().name());
    assertEquals(List.of("file"),
        factory.provides().params().stream().map(parameter -> parameter.name()).toList());
  }

  @Test
  void theFeatureCensusesBesideTheMapItWasGiven() throws IOException {
    final Path path = map("io.sirix.api\tNodeTrx.java\t24");
    final MutationInterceptor interceptor =
        new ChangedLineInterceptorFactory().createInterceptor(parameters(path.toString()));

    run(interceptor, List.of(mutant("io.sirix.api.NodeTrx", "NodeTrx.java", 36, 1)));

    assertEquals(path.resolveSibling(ChangedLineInterceptorFactory.CENSUS_FILE), census());
    assertEquals(1, Files.readAllLines(census()).size());
    assertEquals(InterceptorType.PRE_SCAN_FILTER, interceptor.type());
  }

  private static InterceptorParameters parameters(final @Nullable String file) {
    final Map<String, List<String>> settings =
        file == null ? Map.of() : Map.of("file", List.of(file));
    return new InterceptorParameters(
        new FeatureSetting("CHANGEDLINES", ToggleStatus.ACTIVATE, settings), null, null, null,
        null, null, null);
  }

  /** A {@link InterceptorType#FILTER} interceptor that records what the pipeline handed it. */
  private static final class RecordingFilter implements MutationInterceptor {

    private final List<MutationDetails> seen = new ArrayList<>();

    @Override
    public InterceptorType type() {
      return InterceptorType.FILTER;
    }

    @Override
    public void begin(final @Nullable ClassTree clazz) {
      // Nothing to collect.
    }

    @Override
    public Collection<MutationDetails> intercept(final Collection<MutationDetails> mutations,
        final @Nullable Mutater mutater) {
      seen.addAll(mutations);
      return mutations;
    }

    @Override
    public void end() {
      // Nothing to release.
    }
  }
}
