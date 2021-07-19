package org.sirix.xquery.json;

import com.google.gson.stream.JsonReader;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.JsonStore;
import org.sirix.api.Database;
import org.sirix.api.json.JsonResourceManager;

import java.nio.file.Path;
import java.util.Set;

/**
 * Database store.
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public interface JsonDBStore extends JsonStore, AutoCloseable {
  JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceManager> database);

  JsonDBStore removeDatabase(Database<JsonResourceManager> database);

  @Override
  JsonDBCollection lookup(String name);

  @Override
  JsonDBCollection create(String name);

  @Override
  JsonDBCollection create(String collName, Path path);

  @Override
  JsonDBCollection createFromPaths(String collName, Stream<Path> path);

  @Override
  JsonDBCollection create(String collName, String optResName, Path path);

  @Override
  JsonDBCollection create(String collName, String path);

  @Override
  JsonDBCollection create(String collName, String optResName, String json);

  JsonDBCollection create(String collName, String optResName, JsonReader json);

  JsonDBCollection create(String collName, Set<JsonReader> json);

  @Override
  JsonDBCollection createFromJsonStrings(String collName, Stream<Str> json);

  @Override
  void drop(String name);

  @Override
  void makeDir(String path);

  @Override
  void close();
}
