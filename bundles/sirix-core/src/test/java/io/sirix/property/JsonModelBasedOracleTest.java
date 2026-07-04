package io.sirix.property;

import io.sirix.JsonTestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Deterministic model-based oracle runs: fixed seeds replay known operation sequences on every
 * execution (see {@link JsonModelOracleHarness} for the oracle itself). Deliberately contains
 * ONLY fixed-seed runs so it can serve as a PIT mutation-testing target — PIT requires a suite
 * that is green without mutation, which randomly-seeded runs cannot guarantee. Fresh random
 * exploration lives in {@link JsonModelBasedOracleRandomTest}.
 *
 * <p>When a random run finds a failing seed, add that seed here as a permanent regression entry.
 */
final class JsonModelBasedOracleTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /** Fixed seeds so every CI run replays known operation sequences deterministically. */
  @ParameterizedTest
  @ValueSource(longs = {1L, 42L, 4711L, 987654321L, -1348769044L})
  void fixedSeedOperationSequenceMatchesOracle(final long seed) {
    JsonModelOracleHarness.runOracle(seed);
  }
}
