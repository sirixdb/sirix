package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.JsonStore;
import io.brackit.query.jdm.json.Object;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;

import java.nio.file.Path;
import java.util.Set;

/**
 * Database store.
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

  JsonDBCollection create(String collName, Path path, Object options);

  @Override
  JsonDBCollection createFromPaths(String collName, Stream<Path> paths);

  JsonDBCollection create(String collName, String optResName, Path path);

  JsonDBCollection create(String collName, String optResName, Path path, Object options);

  @Override
  JsonDBCollection create(String collName, String path);

  JsonDBCollection create(String collName, String path, Object options);

  JsonDBCollection create(String collName, String optResName, String json);

  JsonDBCollection create(String collName, String optResName, String json, Object options);

  JsonDBCollection create(String collName, String optResName, JsonReader json);

  JsonDBCollection create(String collName, String optResName, JsonReader json, Object options);

  JsonDBCollection create(String collName, Set<JsonReader> json);

  JsonDBCollection create(String collName, Set<JsonReader> json, Object options);

  @Override
  JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons);

  /**
   * Create a collection from a stream of JSON strings, one resource per string, applying the given
   * resource-creation options (e.g. valid-time configuration) to every created resource.
   *
   * @param collName the collection/database name
   * @param jsons the JSON fragments
   * @param options the resource-creation options
   * @return the created collection
   */
  default JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons, Object options) {
    return createFromJsonStrings(collName, jsons);
  }

  @Override
  void drop(String name);

  @Override
  void makeDir(String path);

  @Override
  void close();

  Options options();
}
