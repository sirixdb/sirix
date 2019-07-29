package org.sirix.api.json;

import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.VisitResultType;

public interface JsonNodeReadOnlyTrx extends NodeCursor, NodeReadOnlyTrx {
  @Override
  public String getValue();

  public boolean isObject();

  public boolean isObjectKey();

  public boolean isArray();

  public boolean isStringValue();

  public boolean isNumberValue();

  public boolean isNullValue();

  public boolean isBooleanValue();

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
}
