package io.sirix.query.bench.clickbench;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickBenchRigLeaseCampaignPathTest {
  private static final long GIB = 1L << 30;

  @TempDir
  Path directory;

  @Test
  void aStaleCampaignPointerDoesNotFailAnUnrelatedRun() throws IOException {
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    final Path rotatedAway = directory.resolve("clickbench-seg100m-20260905-2328");
    assertFalse(Files.exists(rotatedAway.resolve("db")));
    assertFalse(ClickBenchRigLease.isCampaignDatabase(rotatedAway.toString(), small));
  }

  @Test
  void anAbsentQueryDatabaseIsNotTheCampaignDatabase() throws IOException {
    final Path campaign = directory.resolve("campaign");
    Files.createDirectories(campaign.resolve("db"));
    assertFalse(ClickBenchRigLease.isCampaignDatabase(campaign.toString(), directory.resolve("absent")));
  }

  @Test
  void theCampaignDatabaseIsRecognisedThroughADifferentPathSpelling() throws IOException {
    final Path campaign = directory.resolve("campaign");
    final Path database = Files.createDirectories(campaign.resolve("db"));
    assertTrue(ClickBenchRigLease.isCampaignDatabase(campaign.toString(), database));
    assertTrue(ClickBenchRigLease.isCampaignDatabase(campaign.toString(), directory.resolve("campaign/./db")));
  }

  @Test
  void aCampaignLoadIsRecognisedBeforeItHasCreatedItsDatabase() throws IOException {
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260908-1200"));
    assertFalse(Files.exists(campaign.resolve("db")));
    assertTrue(ClickBenchRigLease.isCampaignDatabase(campaign.toString(), campaign.resolve("db")));
  }

  @Test
  void anUnsetOrBlankCampaignPointerIsNotTheCampaignDatabase() throws IOException {
    final Path database = Files.createDirectories(directory.resolve("db"));
    assertFalse(ClickBenchRigLease.isCampaignDatabase(null, database));
    assertFalse(ClickBenchRigLease.isCampaignDatabase("  ", database));
  }

  @Test
  void aOneMillionLoadDoesNotTakeTheCampaignLease() throws IOException {
    final Path campaign = directory.resolve("campaign");
    Files.createDirectories(campaign.resolve("db"));
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    assertEquals("", withoutFlockLeases(() -> ClickBenchRigLease.holdForLoadProcess(derived(pointers(campaign), small), small)));
  }

  @Test
  void theCampaignLoadTakesTheCampaignLease() throws IOException {
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260908-1200"));
    final String announced =
        withoutFlockLeases(() -> ClickBenchRigLease.holdForLoadProcess(derived(pointers(campaign), campaign.resolve("db")),
            campaign.resolve("db")));
    assertTrue(announced.contains("rig lease") && announced.contains("Linux"), announced);
  }

  @Test
  void aSmallQueryRunKeepsItsOwnJvmEnvelope() throws IOException {
    final Path campaign = directory.resolve("campaign");
    Files.createDirectories(campaign.resolve("db"));
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    assertEquals("",
        withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(derived(pointers(campaign), small), 24 * GIB, small)));
  }

  @Test
  void aCampaignQueryRunMustMatchTheHundredMillionEnvelope() throws IOException {
    final Path campaign = directory.resolve("campaign");
    final Path database = Files.createDirectories(campaign.resolve("db"));
    final IOException refused = assertThrows(IOException.class,
        () -> withoutFlockLeases(
            () -> ClickBenchRigLease.holdForQueryProcess(derived(pointers(campaign), database), 24 * GIB, database)));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
  }

  @Test
  void thePointerFileNamesTheCampaignDatabaseAheadOfTheEnvironmentVariable() throws IOException {
    final Path work = Files.createDirectories(directory.resolve("rig-work"));
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260908-1200"));
    Files.writeString(work.resolve("current-100m-dir.txt"), campaign + "\n");
    final List<ClickBenchRigLease.CampaignPointer> consulted = ClickBenchRigLease.campaignPointers(work);
    assertEquals(campaign.toString(), consulted.get(0).named());
    assertEquals(work.resolve("current-100m-dir.txt").toString(), consulted.get(0).source());
    assertNotNull(ClickBenchRigLease.campaignMatch(consulted, campaign.resolve("db")));
    assertNull(ClickBenchRigLease.campaignMatch(consulted, Files.createDirectories(directory.resolve("seg1m/db"))));
  }

  @Test
  void aBoxWithNoPointerFileConsultsNothingFromDisk() throws IOException {
    final Path work = Files.createDirectories(directory.resolve("empty-work"));
    final Path database = Files.createDirectories(directory.resolve("scratch/db"));
    assertNull(ClickBenchRigLease.campaignMatch(ClickBenchRigLease.campaignPointers(work), database));
  }

  @Test
  void aRotatedPointerDoesNotStopALaterSourceFromMakingTheRunExclusive() throws IOException {
    final Path rotatedAway = directory.resolve("clickbench-seg100m-20260905-2328");
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260908-1200"));
    final Path database = Files.createDirectories(campaign.resolve("db"));
    assertFalse(Files.exists(rotatedAway));
    final List<ClickBenchRigLease.CampaignPointer> consulted = pointers(rotatedAway, campaign);
    assertEquals(campaign.toString(), ClickBenchRigLease.campaignMatch(consulted, database).named());
    final IOException refused = assertThrows(IOException.class,
        () -> withoutFlockLeases(
            () -> ClickBenchRigLease.holdForQueryProcess(derived(consulted, database), 24 * GIB, database)));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
  }

  @Test
  void aRawRunTheChainCannotPlaceStaysShared() throws IOException {
    final Path database = Files.createDirectories(directory.resolve("scratch/db"));
    assertEquals("", withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(derived(List.of(), database), 24 * GIB, database)));
  }

  /** The decision the pointer chain alone implies, as a raw entry point with no parent reaches it. */
  private static ClickBenchRigLease.Decision derived(final List<ClickBenchRigLease.CampaignPointer> consulted,
      final Path database) throws IOException {
    return ClickBenchRigLease.decide(consulted, database);
  }

  @Test
  void aLauncherDecisionOfCampaignHoldsWhereThePointerChainWouldDemoteTheRun() throws IOException {
    // Two sources naming different existing directories is the state that produced a phantom
    // regression: the launcher placed the run as campaign through the second source, while the JVM
    // read only the first and demoted itself to a shared lease beside another 100M benchmark.
    final Path previous = directory.resolve("clickbench-seg100m-20260905-2328");
    final Path campaign = directory.resolve("clickbench-seg100m-20260909-1200");
    Files.createDirectories(previous.resolve("db"));
    final Path database = Files.createDirectories(campaign.resolve("db"));
    assertFalse(ClickBenchRigLease.decide(pointers(previous), database).campaign(),
                "the chain the JVM can see must be the one that would demote this run");

    final ClickBenchRigLease.Decision inherited =
        ClickBenchRigLease.inheritedDecision("campaign", database.toString(), database);
    assertTrue(inherited.campaign());
    assertEquals(database.toString(), inherited.named());
    assertEquals("CB_RIG_CLASSIFICATION", inherited.source());
    final IOException refused = assertThrows(IOException.class,
        () -> withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(inherited, 24 * GIB, database)));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
  }

  @Test
  void aDecisionTakenForAnotherDatabaseNeverReclassifiesThisOne() throws IOException {
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260909-1200"));
    final Path elsewhere = Files.createDirectories(campaign.resolve("db"));
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    assertNull(ClickBenchRigLease.inheritedDecision("campaign", elsewhere.toString(), small),
               "a decision naming another database must be ignored, not applied");
    // Ignored means this run resolves for itself, and the chain places it as an unrelated database.
    assertEquals("", withoutFlockLeases(
        () -> ClickBenchRigLease.holdForQueryProcess(derived(pointers(campaign), small), 24 * GIB, small)));
  }

  @Test
  void aRawRunWithNoInheritedDecisionStillClassifiesFromTheChain() throws IOException {
    final Path campaign = directory.resolve("clickbench-seg100m-20260909-1200");
    final Path database = Files.createDirectories(campaign.resolve("db"));
    assertNull(ClickBenchRigLease.inheritedDecision(null, null, database));
    assertNull(ClickBenchRigLease.inheritedDecision("campaign", "  ", database));
    final ClickBenchRigLease.Decision decision = ClickBenchRigLease.decide(pointers(campaign), database);
    assertTrue(decision.campaign());
    assertEquals("CB100M_DIR", decision.source());
    final IOException refused = assertThrows(IOException.class,
        () -> withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(decision, 24 * GIB, database)));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
  }

  @Test
  void aRetargetableAliasNeverCarriesADecisionOntoAnotherDatabase() throws IOException {
    // Operators keep a stable alias pointing at whichever campaign directory the last load wrote,
    // so the JVM must compare the conclusion it inherited against the database that conclusion was
    // reached about, not against the spelling the alias happened to have at the time.
    final Path scratch = directory.resolve("scratch");
    final Path campaign = directory.resolve("clickbench-seg100m-20260909-1200");
    Files.createDirectories(scratch);
    Files.createDirectories(campaign.resolve("db"));
    final Path alias = Files.createSymbolicLink(directory.resolve("current"), scratch);
    // What a launcher that decided about `current/db` exports: the database it reached the
    // conclusion about, named canonically, exactly as runtime.classify_target records it.
    final String decided = scratch.toRealPath().resolve("db").toString();

    // The scratch load names its database before creating it, and its own run still inherits.
    final ClickBenchRigLease.Decision inherited =
        ClickBenchRigLease.inheritedDecision("other", decided, alias.resolve("db"));
    assertNotNull(inherited, "a conclusion must still name the database it was reached about");
    assertFalse(inherited.campaign());

    Files.delete(alias);
    Files.createSymbolicLink(directory.resolve("current"), campaign);
    assertNull(ClickBenchRigLease.inheritedDecision("other", decided, alias.resolve("db")),
               "a repointed alias must not carry an `other` verdict onto the campaign database");
    // Ignored means this run derives for itself, and the chain places it as the campaign database.
    final ClickBenchRigLease.Decision derived = derived(pointers(campaign), alias.resolve("db"));
    assertTrue(derived.campaign());
    final IOException refused = assertThrows(IOException.class,
        () -> withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(derived, 24 * GIB, alias.resolve("db"))));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
  }

  @Test
  void aLauncherDecisionOfOtherNamesNoCampaignDatabaseAndSharesTheHost() throws IOException {
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    final ClickBenchRigLease.Decision inherited =
        ClickBenchRigLease.inheritedDecision("other", small.toString(), small);
    assertFalse(inherited.campaign());
    assertEquals("unset", inherited.named());
    assertEquals("", withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(inherited, 24 * GIB, small)));
  }

  private static List<ClickBenchRigLease.CampaignPointer> pointers(final Path... named) {
    return Arrays.stream(named)
                 .map(path -> new ClickBenchRigLease.CampaignPointer(path.toString(), "CB100M_DIR"))
                 .toList();
  }

  /**
   * Runs a lease acquisition as if on a host without process-owned flock leases, so a test can
   * observe the acquisition decision without ever touching the machine's shared benchmark locks.
   */
  private static String withoutFlockLeases(final Acquisition acquisition) throws IOException {
    final String platform = System.getProperty("os.name", "");
    final PrintStream out = System.out;
    final ByteArrayOutputStream captured = new ByteArrayOutputStream();
    System.setProperty("os.name", "Mac OS X");
    try {
      assertFalse(System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux"));
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
      acquisition.hold();
      return captured.toString(StandardCharsets.UTF_8);
    } finally {
      System.setOut(out);
      System.setProperty("os.name", platform);
    }
  }

  @FunctionalInterface
  private interface Acquisition {
    void hold() throws IOException;
  }
}
