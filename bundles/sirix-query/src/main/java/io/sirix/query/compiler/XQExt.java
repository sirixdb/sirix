package io.sirix.query.compiler;

import io.brackit.query.compiler.XQ;

/**
 * @author Sebastian Baechle
 * 
 */
public final class XQExt {

  private static final int OFFSET = XQ.allocate(5);

  public static final int MultiStepExpr = OFFSET;

  public static final int IndexExpr = OFFSET + 1;

  public static final int ParentExpr = OFFSET + 2;

  public static final int VectorizedPipelineExpr = OFFSET + 3;

  public static final int HashMembershipJoin = OFFSET + 4;

  public static final String NAMES[] =
      new String[] {"MultiStepExpr", "IndexExpr", "ParentExpr", "VectorizedPipelineExpr", "HashMembershipJoin"};

  public static Object toName(int key) {
    return NAMES[key - OFFSET];
  }
}
