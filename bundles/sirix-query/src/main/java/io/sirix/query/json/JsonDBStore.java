package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.json.JsonStore;
import io.brackit.query.jdm.json.Object;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;

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

	@Override
	void drop(String name);

	@Override
	void makeDir(String path);

	@Override
	void close();

	Options options();
}
