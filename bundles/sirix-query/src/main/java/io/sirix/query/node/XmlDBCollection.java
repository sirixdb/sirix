package io.sirix.query.node;

import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.node.AbstractTemporalNode;
import io.brackit.query.jdm.node.TemporalNodeCollection;
import io.brackit.query.node.parser.NodeSubtreeParser;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlResourceSession;

import java.time.Instant;

/**
 * An XML database collection backed by a Sirix {@link Database}.
 *
 * <p>This is an interface (implemented by {@link XmlDBCollectionImpl}) so that cross-cutting
 * concerns can be layered on by composition/delegation rather than by subclassing the concrete
 * implementation. In particular the REST layer wraps a collection in an authorization-checking
 * decorator that delegates every read to the real collection and re-checks the caller's role on
 * the mutating methods.
 *
 * <p>The brackit {@link TemporalNodeCollection} supertype already declares the standard collection
 * operations ({@code getDocument*}, {@code getDocuments}, {@code getDocumentCount}, {@code add},
 * {@code delete}, {@code remove}, {@code getName}); this interface only adds the Sirix-specific
 * accessors and the resource-named {@code add} overloads.
 */
public interface XmlDBCollection extends TemporalNodeCollection<AbstractTemporalNode<XmlDBNode>>, AutoCloseable {

  /**
   * Get the underlying Sirix {@link Database}.
   *
   * @return the Sirix {@link Database}
   */
  Database<XmlResourceSession> getDatabase();

  /**
   * Get the unique ID.
   *
   * @return the unique ID
   */
  int getID();

  /**
   * Add a resource to the collection from a subtree parser.
   *
   * @param resourceName the resource name
   * @param parser the subtree parser
   * @return the stored document node
   */
  XmlDBNode add(String resourceName, NodeSubtreeParser parser);

  /**
   * Add a resource to the collection from a subtree parser with a custom commit.
   *
   * @param resourceName the resource name
   * @param parser the subtree parser
   * @param commitMessage the commit message
   * @param commitTimestamp the commit timestamp
   * @return the stored document node
   */
  XmlDBNode add(String resourceName, NodeSubtreeParser parser, String commitMessage, Instant commitTimestamp);

  // The concrete implementation narrows the brackit collection element type
  // (AbstractTemporalNode<XmlDBNode>) to XmlDBNode; re-declare the read/add operations with the
  // narrowed return type so callers holding the interface type keep seeing XmlDBNode.

  @Override
  XmlDBNode getDocument();

  @Override
  XmlDBNode getDocument(int revision);

  @Override
  XmlDBNode getDocument(Instant pointInTime);

  @Override
  XmlDBNode getDocument(String name);

  @Override
  XmlDBNode getDocument(String name, int revision);

  @Override
  XmlDBNode getDocument(String name, Instant pointInTime);

  @Override
  Stream<XmlDBNode> getDocuments();

  @Override
  XmlDBNode add(NodeSubtreeParser parser);

  @Override
  void close();
}
