package io.sirix.mcp;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Maps human-readable snapshot labels to SirixDB revision numbers.
 *
 * <p>SirixDB has no native named-snapshot concept — revisions are just integers.
 * This registry provides the named labels that agents need for the
 * snapshot/experiment/discard workflow.
 *
 * <p>Labels are just pointers — zero storage cost. Deleting a label does not
 * delete the revision (CoW pages are shared).
 *
 * <p>Persisted as JSON per database to {@code <db-path>/.sirix-mcp-snapshots.json}.
 */
public final class SnapshotRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegistry.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final String SNAPSHOT_FILE = ".sirix-mcp-snapshots.json";
  private static final Pattern VALID_NAME = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$");

  // database -> resource -> (label -> revision)
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>> registry =
      new ConcurrentHashMap<>();

  private final Path basePath;

  public SnapshotRegistry(Path basePath) {
    this.basePath = basePath;
  }

  /** Load snapshots for a database from disk. */
  public void loadForDatabase(String database) {
    var file = basePath.resolve(database).resolve(SNAPSHOT_FILE);
    if (Files.exists(file)) {
      try {
        var data = MAPPER.readValue(
            Files.readString(file),
            new TypeReference<Map<String, Map<String, Integer>>>() {});

        var dbMap = registry.computeIfAbsent(database, k -> new ConcurrentHashMap<>());
        data.forEach((resource, snapshots) -> {
          var resMap = dbMap.computeIfAbsent(resource, k -> new ConcurrentHashMap<>());
          resMap.putAll(snapshots);
        });

        LOG.info("Loaded {} snapshot(s) for database '{}'",
            data.values().stream().mapToInt(Map::size).sum(), database);
      } catch (IOException e) {
        LOG.warn("Failed to load snapshots for database '{}': {}", database, e.getMessage());
      }
    }
  }

  /** Create a named snapshot pointing to a revision. */
  public void create(String database, String resource, String name, int revision) {
    validateName(name);
    registry
        .computeIfAbsent(database, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(resource, k -> new ConcurrentHashMap<>())
        .put(name, revision);
    persist(database);
  }

  /** Delete a named snapshot. Returns true if it existed. */
  public boolean delete(String database, String resource, String name) {
    var dbMap = registry.get(database);
    if (dbMap == null) return false;
    var resMap = dbMap.get(resource);
    if (resMap == null) return false;
    boolean removed = resMap.remove(name) != null;
    if (removed) {
      persist(database);
    }
    return removed;
  }

  /** Resolve a snapshot name to a revision number. Returns null if not found. */
  public Integer resolve(String database, String resource, String name) {
    var dbMap = registry.get(database);
    if (dbMap == null) return null;
    var resMap = dbMap.get(resource);
    if (resMap == null) return null;
    return resMap.get(name);
  }

  /** List all snapshots for a resource. */
  public Map<String, Integer> list(String database, String resource) {
    var dbMap = registry.get(database);
    if (dbMap == null) return Map.of();
    var resMap = dbMap.get(resource);
    if (resMap == null) return Map.of();
    return Map.copyOf(resMap);
  }

  static void validateName(String name) {
    if (name == null || !VALID_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid snapshot name: must be 1-128 chars, alphanumeric/dash/underscore/dot, "
              + "starting with alphanumeric");
    }
  }

  private void persist(String database) {
    var dbMap = registry.get(database);
    if (dbMap == null) return;

    var file = basePath.resolve(database).resolve(SNAPSHOT_FILE);
    try {
      Files.createDirectories(file.getParent());
      var tmp = file.resolveSibling(SNAPSHOT_FILE + ".tmp");
      MAPPER.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), dbMap);
      try {
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      LOG.warn("Failed to persist snapshots for database '{}': {}", database, e.getMessage());
    }
  }
}
