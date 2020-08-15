package org.sirix.api.json;

import com.google.gson.JsonObject;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.node.SirixDeweyID;

import java.util.List;

public interface JsonNodeReadOnlyTrx extends NodeCursor, NodeReadOnlyTrx {
  @Override
  String getValue();

  boolean isObject();

  boolean isObjectKey();

  boolean isArray();

  boolean isStringValue();

  boolean isNumberValue();

  boolean isNullValue();

  boolean isBooleanValue();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveTo(long nodeKey);

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToDocumentRoot();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToFirstChild();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToLastChild();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToLeftSibling();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToParent();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToRightSibling();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToPrevious();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToNext();

  @Override
  Move<? extends JsonNodeReadOnlyTrx> moveToNextFollowing();

  /**
   * Accept a visitor.
   *
   * @param visitor {@link JsonNodeVisitor} implementation
   * @return {@link VisitResultType} value
   */
  VisitResult acceptVisitor(JsonNodeVisitor visitor);

  /**
   * Get the {@link ResourceManager} this instance is bound to.
   *
   * @return the resource manager
   */
  @Override
  JsonResourceManager getResourceManager();

  boolean getBooleanValue();

  Number getNumberValue();

  int getNameKey();

  List<JsonObject> getUpdateOperations();

  List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth);
}
