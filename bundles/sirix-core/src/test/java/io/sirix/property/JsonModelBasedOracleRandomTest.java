package io.sirix.property;

import io.sirix.JsonTestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

/**
 * Randomly-seeded model-based oracle exploration (see {@link JsonModelOracleHarness}). Each run
 * explores fresh operation sequences; on failure the seed and full operation log are printed —
 * add the failing seed to {@link JsonModelBasedOracleTest}'s fixed-seed list as a permanent
 * regression entry.
 *
 * <p>Kept SEPARATE from the fixed-seed class on purpose: this class must NOT be a PIT
 * mutation-testing target ({@code targetTests} in {@code bundles/sirix-core/build.gradle}),
 * because PIT demands a suite that is green without mutation, and a randomly-seeded run can
 * legitimately fail by discovering a real, previously unknown bug.
 */
final class JsonModelBasedOracleRandomTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @RepeatedTest(10)
  void randomOperationSequenceMatchesOracle() {
    JsonModelOracleHarness.runOracle(System.nanoTime());
  }
}
