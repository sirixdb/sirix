package io.sirix.api.json;

import com.google.gson.JsonObject;
import io.brackit.query.atomic.QNm;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.node.SirixDeweyID;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.ResourceSession;

import java.nio.charset.StandardCharsets;
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
   * @return the resource session
   */
  @Override
  JsonResourceSession getResourceSession();

  boolean getBooleanValue();

  Number getNumberValue();

  int getNameKey();

  /**
   * The current node's object-key name as the UTF-8 bytes held by the name dictionary, or
   * {@code null} when the node has no name. Unlike {@link #getName()} this decodes no String and
   * allocates no {@link QNm} — the dictionary's own array is handed back, so callers MUST treat
   * it as read-only and MUST NOT retain it across a cursor move.
   *
   * <p>Declared here rather than on the shared cursor interface deliberately: the XML side
   * already has its own raw-name API ({@code XmlNodeReadOnlyTrx#rawNameForKey(int)}) and a
   * core-wide default would hand XML callers a silently allocating impostor of this contract.
   * The default below derives the bytes from {@link #getName()} and therefore allocates; the
   * real cursor overrides it with the dictionary lookup.
   *
   * @return the name's raw UTF-8 bytes, or {@code null} if the node has no name
   */
  default byte[] getNameBytes() {
    final QNm name = getName();
    return name == null ? null : name.getLocalName().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * @return {@code true} when the cursor is currently in the synthetic primitive-value
   *         child mode of a fused {@code OBJECT_NAMED_*} record (iter#30). Used by
   *         axes / translators that need to distinguish the virtual child from an actual
   *         primitive node.
   */
  boolean isFusedSyntheticChild();

  List<JsonObject> getUpdateOperations();

  List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth);
}
