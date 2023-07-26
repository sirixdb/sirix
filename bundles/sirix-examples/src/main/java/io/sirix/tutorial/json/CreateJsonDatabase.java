package io.sirix.tutorial.json;

import io.sirix.tutorial.Constants;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.service.json.shredder.JsonShredder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class CreateJsonDatabase {

  private static final Path JSON = Paths.get("src", "main", "resources", "json");

  public static void main(String[] args) {
    createJsonDatabase();
  }

  static void createJsonDatabase() {
    final var pathToJsonFile = JSON.resolve("complex1.json");
    final var databaseFile = Constants.SIRIX_DATA_LOCATION.resolve("json-database");

    if (Files.exists(databaseFile))
      Databases.removeDatabase(databaseFile);

    final var dbConfig = new DatabaseConfiguration(databaseFile);
    Databases.createJsonDatabase(dbConfig);
    try (final var database = Databases.openJsonDatabase(databaseFile)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .useTextCompression(false)
                                                   .useDeweyIDs(true)
                                                   .build());
      try (final var manager = database.beginResourceSession("resource"); final var wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(pathToJsonFile));
        wtx.commit();
      }
    }

    System.out.println("Database with resource created.");
  }
}
