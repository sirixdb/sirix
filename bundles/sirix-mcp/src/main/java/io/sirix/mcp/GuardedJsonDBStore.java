package io.sirix.mcp;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.Object;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.Options;

import java.nio.file.Path;
import java.util.Set;

/**
 * Decorator around {@link JsonDBStore} that enforces MCP access control on all store operations.
 *
 * <p>This prevents query injection via {@code jn:store()}, {@code jn:doc()},
 * {@code jn:drop-database()} etc. Same pattern as the REST API's {@code JsonSessionDBStore}.
 */
public final class GuardedJsonDBStore implements JsonDBStore {

  private final JsonDBStore delegate;
  private final AccessControl accessControl;

  public GuardedJsonDBStore(JsonDBStore delegate, AccessControl accessControl) {
    this.delegate = delegate;
    this.accessControl = accessControl;
  }

  // --- Lookup: check database access ---

  @Override
  public JsonDBCollection lookup(String name) {
    accessControl.checkDatabaseAccess(name);
    return delegate.lookup(name);
  }

  // --- Create overloads: check write + database access ---

  @Override
  public JsonDBCollection create(String name) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(name);
    return delegate.create(name);
  }

  @Override
  public JsonDBCollection create(String collName, Path path) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, path);
  }

  @Override
  public JsonDBCollection create(String collName, Path path, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, path, options);
  }

  @Override
  public JsonDBCollection createFromPaths(String collName, Stream<Path> paths) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.createFromPaths(collName, paths);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, Path path) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, path);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, Path path, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, path, options);
  }

  @Override
  public JsonDBCollection create(String collName, String path) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, path);
  }

  @Override
  public JsonDBCollection create(String collName, String path, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, path, options);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, json);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, json, options);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, JsonReader json) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, json);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, JsonReader json, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, optResName, json, options);
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> json) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, json);
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> json, Object options) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.create(collName, json, options);
  }

  @Override
  public JsonDBCollection createFromJsonStrings(String collName, Stream<Str> jsons) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(collName);
    return delegate.createFromJsonStrings(collName, jsons);
  }

  // --- Drop: check write + database access ---

  @Override
  public void drop(String name) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(name);
    delegate.drop(name);
  }

  // --- MakeDir: check write access ---

  @Override
  public void makeDir(String path) {
    accessControl.checkWriteAccess();
    delegate.makeDir(path);
  }

  // --- Pass-through operations ---

  @Override
  public JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceSession> database) {
    return delegate.addDatabase(jsonDBCollection, database);
  }

  @Override
  public JsonDBStore removeDatabase(Database<JsonResourceSession> database) {
    return delegate.removeDatabase(database);
  }

  @Override
  public Options options() {
    return delegate.options();
  }

  @Override
  public void close() {
    delegate.close();
  }
}
