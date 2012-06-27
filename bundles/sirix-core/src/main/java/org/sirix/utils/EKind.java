package org.sirix.utils;

import java.util.HashMap;
import java.util.Map;

import org.sirix.node.ENode;
import org.sirix.node.interfaces.INode;


public class EKind {
  
  private static final Map<Byte, ENode> INSTANCEFORID = new HashMap<>();
  
  /** Mapping of class -> nodes. */
  private static final Map<Class<? extends INode>, ENode> INSTANCEFORCLASS = new HashMap<>();
 
  static {
    for (final ENode node : ENode.values()) {
      INSTANCEFORID.put(node.getId(), node);
      INSTANCEFORCLASS.put(node.getNodeClass(), node);
    }
  }
  
  /**
   * Public method to get the related node based on the identifier.
   * 
   * @param pId
   *          the identifier for the node
   * @return the related node
   */
  public static ENode getKind(final byte pId) {
    return INSTANCEFORID.get(pId);
  }

  /**
   * Public method to get the related node based on the class.
   * 
   * @param pClass
   *          the class for the node
   * @return the related node
   */
  public static ENode getKind(final Class<? extends INode> pClass) {
    return INSTANCEFORCLASS.get(pClass);
  }
}
