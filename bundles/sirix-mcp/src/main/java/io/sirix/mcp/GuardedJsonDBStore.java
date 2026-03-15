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
 * A {@link JsonDBStore} decorator that enforces MCP access control on all store operations.
 *
 * <p>This mirrors the pattern used by the REST API's {@code JsonSessionDBStore},
 * but without Vert.x or Keycloak dependencies. Query functions like {@code jn:store()},
 * {@code jn:doc()}, and {@code jn:drop-database()} call store methods directly —
 * this wrapper ensures those calls are subject to the same access control as
 * direct MCP tool invocations.
 *
 * <p>In read-only mode (default), all {@code create} and {@code drop} methods
 * throw {@link AccessControl.AccessDeniedException}.
 */
public final class GuardedJsonDBStore implements JsonDBStore {

  private final JsonDBStore delegate;
  private final AccessControl accessControl;

  public GuardedJsonDBStore(JsonDBStore delegate, AccessControl accessControl) {
    this.delegate = delegate;
    this.accessControl = accessControl;
  }

  @Override
  public JsonDBCollection lookup(String name) {
    accessControl.checkDatabaseAccess(name);
    return delegate.lookup(name);
  }

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
  public JsonDBCollection create(String collName, String optResName, JsonReader json,
      Object options) {
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

  @Override
  public void drop(String name) {
    accessControl.checkWriteAccess();
    accessControl.checkDatabaseAccess(name);
    delegate.drop(name);
  }

  @Override
  public void makeDir(String path) {
    accessControl.checkWriteAccess();
    delegate.makeDir(path);
  }

  @Override
  public JsonDBStore addDatabase(JsonDBCollection jsonDBCollection,
      Database<JsonResourceSession> database) {
    delegate.addDatabase(jsonDBCollection, database);
    return this;
  }

  @Override
  public JsonDBStore removeDatabase(Database<JsonResourceSession> database) {
    delegate.removeDatabase(database);
    return this;
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
