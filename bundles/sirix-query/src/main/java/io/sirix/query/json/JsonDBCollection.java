package io.sirix.query.json;

import com.google.gson.stream.JsonReader;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.json.TemporalJsonCollection;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;

/**
 * A JSON database collection backed by a Sirix {@link Database}.
 *
 * <p>This is an interface (implemented by {@link JsonDBCollectionImpl}) so that cross-cutting
 * concerns can be layered on by composition/delegation rather than by subclassing the concrete
 * implementation. In particular the REST layer wraps a collection in an authorization-checking
 * decorator that delegates every read to the real collection and re-checks the caller's role on
 * the mutating methods.
 *
 * <p>The brackit {@link TemporalJsonCollection} supertype already declares the standard collection
 * operations ({@code getDocument*}, {@code getDocuments}, {@code getDocumentCount}, {@code add},
 * {@code delete}, {@code remove}, {@code getName}); this interface only adds the Sirix-specific
 * accessors and the resource-named {@code add} overloads.
 */
public interface JsonDBCollection extends TemporalJsonCollection<JsonDBItem>, AutoCloseable {

  /**
   * Get the underlying Sirix {@link Database}.
   *
   * @return the Sirix {@link Database}
   */
  Database<JsonResourceSession> getDatabase();

  /**
   * Get the unique ID.
   *
   * @return the unique ID
   */
  int getID();

  /**
   * Get the {@link JsonDBStore} this collection belongs to.
   *
   * @return the {@link JsonDBStore}, or {@code null} if not set
   */
  JsonDBStore getJsonDBStore();

  /**
   * Set the {@link JsonDBStore} this collection belongs to.
   *
   * @param jsonDBStore the store
   * @return this collection
   */
  JsonDBCollection setJsonDBStore(JsonDBStore jsonDBStore);

  /**
   * Add a resource to the collection from a JSON reader with the given options.
   *
   * @param resourceName the resource name
   * @param reader the JSON reader
   * @param options the resource options
   * @return the stored document
   */
  JsonDBItem add(String resourceName, JsonReader reader, Object options);

  /**
   * Add a resource to the collection from a JSON reader.
   *
   * @param resourceName the resource name
   * @param reader the JSON reader
   * @return the stored document
   */
  JsonDBItem add(String resourceName, JsonReader reader);

  @Override
  void close();
}
