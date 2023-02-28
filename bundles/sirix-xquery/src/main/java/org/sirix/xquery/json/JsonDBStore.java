package org.sirix.xquery.json;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.jdm.Stream;
import org.brackit.xquery.jdm.json.JsonStore;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceSession;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;

/**
 * Database store.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public interface JsonDBStore extends JsonStore, AutoCloseable {
  JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceSession> database);

  JsonDBStore removeDatabase(Database<JsonResourceSession> database);

  @Override
  JsonDBCollection lookup(String name);

  @Override
  JsonDBCollection create(String name);

  @Override
  JsonDBCollection create(String collName, Path path);

  JsonDBCollection create(String collName, Path path, String commitMessage, Instant commitTimestamp);

  @Override
  JsonDBCollection createFromPaths(String collName, Stream<Path> paths);

  JsonDBCollection create(String collName, String optResName, Path path);

  JsonDBCollection create(String collName, String optResName, Path path, String commitMessage, Instant commitTimestamp);

  @Override
  JsonDBCollection create(String collName, String path);

  JsonDBCollection create(String collName, String path, String commitMessage, Instant commitTimestamp);

  JsonDBCollection create(String collName, String optResName, String json);

  JsonDBCollection create(String collName, String optResName, String json, String commitMessage, Instant commitTimestamp);

  JsonDBCollection create(String collName, String optResName, JsonReader json);

  JsonDBCollection create(String collName, String optResName, JsonReader json, String commitMessage, Instant commitTimestamp);

  JsonDBCollection create(String collName, Set<JsonReader> json);

  @Override
  JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons);

  @Override
  void drop(String name);

  @Override
  void makeDir(String path);

  @Override
  void close();
}
