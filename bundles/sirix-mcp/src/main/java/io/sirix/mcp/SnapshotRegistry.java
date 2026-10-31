package io.sirix.mcp;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import io.sirix.access.DatabaseConfiguration;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Maps human-readable snapshot labels to SirixDB revision numbers.
 *
 * <p>
 * SirixDB has no native named-snapshot concept — revisions are just integers. This registry
 * provides the named labels that agents need for the snapshot/experiment/discard workflow.
 *
 * <p>
 * Labels are just pointers — zero storage cost. Deleting a label does not delete the revision (CoW
 * pages are shared).
 *
 * <p>
 * Persisted as JSON per database to {@code <db-path>/.sirix-mcp-snapshots.json}.
 */
public final class SnapshotRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotRegistry.class);
  private static final JsonMapper MAPPER = JsonMapper.builder().build();
  private static final String SNAPSHOT_FILE = ".sirix-mcp-snapshots.json";
  private static final Pattern VALID_NAME = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$");

  // database -> resource -> (label -> revision)
  private final ConcurrentHashMap<Path, ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>> registry =
      new ConcurrentHashMap<>();

  private final AccessControl accessControl;
  private final Set<Path> loaded = ConcurrentHashMap.newKeySet();

  public SnapshotRegistry(AccessControl accessControl) {
    this.accessControl = requireNonNull(accessControl);
  }

  private Path resourcePath(final String database, final String resource) {
    return accessControl.resourcePath(database, resource, accessControl.databasePath(database));
  }

  private void loadForDatabase(final Path database) {
    if (loaded.contains(database))
      return;
    var file = database.resolve(SNAPSHOT_FILE);
    if (Files.exists(file)) {
      try {
        var data = MAPPER.readValue(Files.readString(file), new TypeReference<Map<String, Map<String, Integer>>>() {});

        final var dbMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();
        data.forEach((resource, snapshots) -> {
          validateName(resource);
          try {
            AccessControl.namedPath(database.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile()), resource,
                "resource");
            dbMap.put(resource, new ConcurrentHashMap<>(snapshots));
          } catch (final AccessControl.AliasedNameException ignored) {
            return;
          }
        });
        registry.put(database, dbMap);

        LOG.info("Loaded {} snapshot(s) for database '{}'", data.values().stream().mapToInt(Map::size).sum(), database);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to load snapshots for database '" + database + "'", e);
      }
    }
    loaded.add(database);
  }

  /** Create a named snapshot pointing to a revision. */
  public synchronized void create(String database, String resource, String name, int revision) {
    final Path target = resourcePath(database, resource);
    final Path path = target.getParent().getParent();
    loadForDatabase(path);
    if (revision < 1)
      throw new IllegalArgumentException("Revision must be positive");
    validateName(name);
    final var labels = registry.computeIfAbsent(path, k -> new ConcurrentHashMap<>())
                               .computeIfAbsent(target.getFileName().toString(), k -> new ConcurrentHashMap<>());
    final Integer previous = labels.put(name, revision);
    try {
      persist(path);
    } catch (final RuntimeException exception) {
      if (previous == null)
        labels.remove(name);
      else
        labels.put(name, previous);
      throw exception;
    }
  }

  /** Delete a named snapshot. Returns true if it existed. */
  public synchronized boolean delete(String database, String resource, String name) {
    final Path target = resourcePath(database, resource);
    final Path path = target.getParent().getParent();
    loadForDatabase(path);
    var dbMap = registry.get(path);
    if (dbMap == null)
      return false;
    var resMap = dbMap.get(target.getFileName().toString());
    if (resMap == null)
      return false;
    final Integer removed = resMap.remove(name);
    if (removed == null)
      return false;
    try {
      persist(path);
    } catch (final RuntimeException exception) {
      resMap.put(name, removed);
      throw exception;
    }
    return true;
  }

  /** Resolve a snapshot name to a revision number. Returns null if not found. */
  public synchronized Integer resolve(String database, String resource, String name) {
    final Path target = resourcePath(database, resource);
    final Path path = target.getParent().getParent();
    loadForDatabase(path);
    var dbMap = registry.get(path);
    if (dbMap == null)
      return null;
    var resMap = dbMap.get(target.getFileName().toString());
    if (resMap == null)
      return null;
    return resMap.get(name);
  }

  /** List all snapshots for a resource. */
  public synchronized Map<String, Integer> list(String database, String resource) {
    final Path target = resourcePath(database, resource);
    final Path path = target.getParent().getParent();
    loadForDatabase(path);
    var dbMap = registry.get(path);
    if (dbMap == null)
      return Map.of();
    var resMap = dbMap.get(target.getFileName().toString());
    if (resMap == null)
      return Map.of();
    return Map.copyOf(resMap);
  }

  static void validateName(String name) {
    if (name == null || !VALID_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(
          "Invalid snapshot name: must be 1-128 chars, alphanumeric/dash/underscore/dot, "
              + "starting with alphanumeric");
    }
  }

  private void persist(Path database) {
    var dbMap = registry.get(database);
    if (dbMap == null)
      return;

    var file = database.resolve(SNAPSHOT_FILE);
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
      throw new UncheckedIOException("Failed to persist snapshots for database '" + database + "'", e);
    }
  }
}
