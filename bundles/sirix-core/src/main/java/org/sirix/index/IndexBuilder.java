package org.sirix.index;

import java.util.Set;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.XdmNodeVisitor;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.NonStructuralWrapperAxis;

/**
 * Build an index by traversing the current revision.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndexBuilder {

  /**
   * Build the index.
   *
   * @param rtx the current {@link XdmNodeReadTrx}
   * @param builders the index builders
   */
  public static void build(final XdmNodeReadTrx rtx, final Set<XdmNodeVisitor> builders) {
    final long nodeKey = rtx.getNodeKey();
    rtx.moveToDocumentRoot();

    for (@SuppressWarnings("unused")
    final long key : new NonStructuralWrapperAxis(new DescendantAxis(rtx))) {
      for (final XdmNodeVisitor builder : builders) {
        rtx.acceptVisitor(builder);
      }
    }
    rtx.moveTo(nodeKey);
  }

  /**
   * Build the index.
   *
   * @param rtx the current {@link XdmNodeReadTrx}
   * @param builders the index builders
   */
  public static void build(final JsonNodeReadOnlyTrx rtx, final Set<JsonNodeVisitor> builders) {
    final long nodeKey = rtx.getNodeKey();
    rtx.moveToDocumentRoot();

    for (@SuppressWarnings("unused")
    final long key : new DescendantAxis(rtx)) {
      for (final JsonNodeVisitor builder : builders) {
        rtx.acceptVisitor(builder);
      }
    }
    rtx.moveTo(nodeKey);
  }

}
