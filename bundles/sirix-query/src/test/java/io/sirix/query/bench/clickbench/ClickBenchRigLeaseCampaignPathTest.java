package io.sirix.query.bench.clickbench;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    assertEquals("", withoutFlockLeases(() -> ClickBenchRigLease.holdForLoadProcess(campaign.toString(), small)));
  }

  @Test
  void theCampaignLoadTakesTheCampaignLease() throws IOException {
    final Path campaign = Files.createDirectories(directory.resolve("clickbench-seg100m-20260908-1200"));
    final String announced =
        withoutFlockLeases(() -> ClickBenchRigLease.holdForLoadProcess(campaign.toString(), campaign.resolve("db")));
    assertTrue(announced.contains("rig lease") && announced.contains("Linux"), announced);
  }

  @Test
  void aSmallQueryRunKeepsItsOwnJvmEnvelope() throws IOException {
    final Path campaign = directory.resolve("campaign");
    Files.createDirectories(campaign.resolve("db"));
    final Path small = Files.createDirectories(directory.resolve("seg1m/db"));
    assertEquals("",
        withoutFlockLeases(() -> ClickBenchRigLease.holdForQueryProcess(campaign.toString(), 24 * GIB, small)));
  }

  @Test
  void aCampaignQueryRunMustMatchTheHundredMillionEnvelope() throws IOException {
    final Path campaign = directory.resolve("campaign");
    final Path database = Files.createDirectories(campaign.resolve("db"));
    final IOException refused = assertThrows(IOException.class, () -> withoutFlockLeases(
        () -> ClickBenchRigLease.holdForQueryProcess(campaign.toString(), 24 * GIB, database)));
    assertTrue(refused.getMessage().contains("100M query envelope"), refused.getMessage());
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
