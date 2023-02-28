package org.sirix.index;

import org.brackit.xquery.jdm.DocumentException;
import org.brackit.xquery.jdm.node.Node;

/**
 * Materializable structure.
 * 
 * @author Sebastian Baechle
 *
 */
public interface Materializable {
  /**
   * Materialize the object as a {@link Node} tree
   * 
   * @return the root of the materialized tree
   * @throws DocumentException
   */
  public Node<?> materialize() throws DocumentException;

  /**
   * Initializes the materialized locator facet
   * 
   * @param root root of the materialized facet subtree
   * @throws DocumentException
   */
  public void init(Node<?> root) throws DocumentException;
}
