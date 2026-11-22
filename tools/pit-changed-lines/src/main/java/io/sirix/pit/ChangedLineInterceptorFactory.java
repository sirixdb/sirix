package io.sirix.pit;

import java.nio.file.Path;
import java.util.Optional;
import org.pitest.mutationtest.build.InterceptorParameters;
import org.pitest.mutationtest.build.MutationInterceptor;
import org.pitest.mutationtest.build.MutationInterceptorFactory;
import org.pitest.plugin.Feature;
import org.pitest.plugin.FeatureParameter;

/**
 * Activates {@link ChangedLineInterceptor} as PIT's {@code CHANGEDLINES} feature (CONV-205).
 *
 * <pre>
 * features = ["+CHANGEDLINES(file[/path/to/changed-lines.tsv])"]
 * </pre>
 *
 * <p>{@code file} is required: a run that asks for this feature without a readable map must fail
 * rather than silently mutate everything or nothing.
 *
 * <p>One parameter, deliberately. PIT 1.25.5's {@code FeatureParser} splits a feature string on
 * {@code ]} and then on {@code [}, so the key of every parameter after the first keeps the comma
 * that separated them and no lookup ever finds it — a second parameter is read as absent, with no
 * error. The census therefore has a derived location rather than a configured one: {@value
 * #CENSUS_FILE}, beside the map the lane wrote. {@code ChangedLineInterceptorTest} pins that
 * parser behaviour, so a release that fixes it fails the test rather than this lane.
 */
public final class ChangedLineInterceptorFactory implements MutationInterceptorFactory {

  /** The census file name, resolved beside the changed-line map. */
  public static final String CENSUS_FILE = "census.jsonl";

  private static final FeatureParameter FILE = FeatureParameter.named("file")
      .withDescription("Path of the changed-line map the lane wrote (required)");

  @Override
  public String description() {
    return "Restricts mutants to the lines a pull request changed";
  }

  @Override
  public Feature provides() {
    return Feature.named("CHANGEDLINES")
        .withDescription("Removes every mutant whose line the pull request did not change")
        .withParameter(FILE);
  }

  @Override
  public MutationInterceptor createInterceptor(final InterceptorParameters parameters) {
    final Optional<String> file = parameters.getString(FILE);
    if (file.isEmpty() || file.get().isBlank()) {
      throw new ChangedLineMap.ChangedLineMapException(
          "CHANGEDLINES needs file[<path>]: the changed-line map the lane wrote");
    }
    final Path map = Path.of(file.get());
    final ChangedLineMap changed = ChangedLineMap.read(map);
    return new ChangedLineInterceptor(changed,
        RestrictionCensus.at(map.resolveSibling(CENSUS_FILE)));
  }
}
