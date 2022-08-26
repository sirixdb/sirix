package org.sirix.api.json;

import com.google.gson.JsonObject;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.ResourceSession;
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
  boolean moveTo(long nodeKey);

  @Override
  boolean moveToDocumentRoot();

  @Override
  boolean moveToFirstChild();

  @Override
  boolean moveToLastChild();

  @Override
  boolean moveToLeftSibling();

  @Override
  boolean moveToParent();

  @Override
  boolean moveToRightSibling();

  @Override
  boolean moveToPrevious();

  @Override
  boolean moveToNext();

  @Override
  boolean moveToNextFollowing();

  /**
   * Accept a visitor.
   *
   * @param visitor {@link JsonNodeVisitor} implementation
   * @return {@link VisitResultType} value
   */
  VisitResult acceptVisitor(JsonNodeVisitor visitor);

  /**
   * Get the {@link ResourceSession} this instance is bound to.
   *
   * @return the resource manager
   */
  @Override
  JsonResourceSession getResourceSession();

  boolean getBooleanValue();

  Number getNumberValue();

  int getNameKey();

  List<JsonObject> getUpdateOperations();

  List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth);
}
