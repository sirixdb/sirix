package io.sirix.query.bench.clickbench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickBenchRigLeaseCampaignPathTest {
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
  void anUnsetOrBlankCampaignPointerIsNotTheCampaignDatabase() throws IOException {
    final Path database = Files.createDirectories(directory.resolve("db"));
    assertFalse(ClickBenchRigLease.isCampaignDatabase(null, database));
    assertFalse(ClickBenchRigLease.isCampaignDatabase("  ", database));
  }
}
