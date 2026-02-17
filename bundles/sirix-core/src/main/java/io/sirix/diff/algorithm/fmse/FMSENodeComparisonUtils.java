package io.sirix.diff.algorithm.fmse;

import javax.xml.namespace.QName;

import io.sirix.access.Utils;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.brackit.query.atomic.QNm;
import io.sirix.node.xml.TextNode;

import java.util.Objects;

class FMSENodeComparisonUtils {

  /**
   * Max length for Levenshtein comparsion.
   */
  private static final int MAX_LENGTH = 50;

  private final long oldStartKey;

  private final long newStartKey;

  private final XmlNodeReadOnlyTrx oldRtx;

  private final XmlNodeReadOnlyTrx newRtx;

  FMSENodeComparisonUtils(final long oldStartKey, final long newStartKey, final XmlNodeReadOnlyTrx oldRtx,
      final XmlNodeReadOnlyTrx newRtx) {
    this.oldStartKey = oldStartKey;
    this.newStartKey = newStartKey;
    this.oldRtx = oldRtx;
    this.newRtx = newRtx;
  }

  /**
   * Compares the values of two nodes. Values are the text content, if the nodes do have child nodes
   * or the name for inner nodes such as element or attribute (an attribute has one child: the value).
   *
   * @param x first node
   * @param y second node
   * @param oldRtx the transactional cursor on the old revision
   * @param newRtx the transactional cursor on the new revision
   * @return true iff the values of the nodes are equal
   */
  boolean nodeValuesEqual(final long x, final long y, final XmlNodeReadOnlyTrx oldRtx,
      final XmlNodeReadOnlyTrx newRtx) {
    assert x >= 0;
    assert y >= 0;
    assert oldRtx != null;
    assert newRtx != null;

    final String a = getNodeValue(x, oldRtx);
    final String b = getNodeValue(y, newRtx);

    return Objects.equals(a, b);
  }

  /**
   * Calculate ratio between 0 and 1 between two String values for {@code text-nodes} denoted.
   *
   * @param oldValue value of first node
   * @param newValue value of second node
   * @return ratio between 0 and 1, whereas 1 is a complete match and 0 denotes that the Strings are
   *         completely different
   */
  float calculateRatio(final String oldValue, final String newValue) {
    assert oldValue != null;
    assert newValue != null;
    float ratio;

    if (oldValue.length() > MAX_LENGTH || newValue.length() > MAX_LENGTH) {
      ratio = Util.quickRatio(oldValue, newValue);
    } else {
      ratio = Levenshtein.getSimilarity(oldValue, newValue);
    }

    return ratio;
  }

  /**
   * Check if ancestors are considered equal (with attributes).
   *
   * @param oldKey start key in old revision
   * @param newKey start key in new revision
   * @return {@code true} if all ancestors up to the start keys are considered equal, {@code false}
   *         otherwise
   */
  boolean checkAncestors(final long oldKey, final long newKey) {
    assert oldKey >= 0;
    assert newKey >= 0;

    oldRtx.moveTo(oldKey);
    newRtx.moveTo(newKey);

    boolean retVal = true;

    if (oldRtx.hasParent() && newRtx.hasParent()) {
      do {
        oldRtx.moveToParent();
        newRtx.moveToParent();

        if (oldRtx.hasAttributes() && newRtx.hasAttributes() && !checkAttributes()) {
          return false;
        }
      } while (oldRtx.getNodeKey() != oldStartKey && newRtx.getNodeKey() != newStartKey && oldRtx.hasParent()
          && newRtx.hasParent() && calculateRatio(getNodeValue(oldRtx.getNodeKey(), oldRtx),
              getNodeValue(newRtx.getNodeKey(), newRtx)) >= 0.7f);
      retVal = (!oldRtx.hasParent() || oldRtx.getNodeKey() == oldStartKey)
          && (!newRtx.hasParent() || newRtx.getNodeKey() == newStartKey);
    } else {
      retVal = false;
    }

    oldRtx.moveTo(oldKey);
    newRtx.moveTo(newKey);

    return retVal;
  }

  boolean checkAttributes() {
    final long newNodeKey = newRtx.getNodeKey();
    final long oldNodeKey = oldRtx.getNodeKey();
    for (int i = 0, attCount = oldRtx.getAttributeCount(); i < attCount; i++) {
      oldRtx.moveToAttribute(i);
      final QNm name = oldRtx.getName();

      if (newRtx.moveToAttributeByName(name) && (calculateRatio(getNodeValue(oldRtx.getNodeKey(), oldRtx),
          getNodeValue(newRtx.getNodeKey(), newRtx)) < 0.7f
          || calculateRatio(oldRtx.getValue(), newRtx.getValue()) < 0.7f)) {
        newRtx.moveTo(newNodeKey);
        oldRtx.moveTo(oldNodeKey);
        return false;
      }

      newRtx.moveTo(newNodeKey);
      oldRtx.moveTo(oldNodeKey);
    }

    newRtx.moveTo(newNodeKey);
    oldRtx.moveTo(oldNodeKey);
    return true;
  }

  /**
   * Check if one of the ancestors has an id.
   *
   * @param oldKey start key in old revision
   * @param newKey start key in new revision
   * @return {@code true} if all ancestors up to the start keys of the FMSE-algorithm, {@code false}
   *         otherwise
   */
  boolean checkIfAncestorIdsMatch(final long oldKey, final long newKey, final QNm id) {
    assert oldKey >= 0;
    assert newKey >= 0;
    oldRtx.moveTo(oldKey);
    newRtx.moveTo(newKey);
    boolean retVal = false;
    if (oldRtx.hasParent() && newRtx.hasParent()) {
      do {
        oldRtx.moveToParent();
        newRtx.moveToParent();

        if (oldRtx.isElement() && newRtx.isElement() && oldRtx.moveToAttributeByName(id)
            && newRtx.moveToAttributeByName(id)) {
          if (newRtx.getValue().equals(oldRtx.getValue()))
            retVal = true;
        } else {
          retVal = false;
          break;
        }
      } while (oldRtx.getNodeKey() != oldStartKey && newRtx.getNodeKey() != newStartKey && oldRtx.hasParent()
          && newRtx.hasParent());
    }

    oldRtx.moveTo(oldKey);
    newRtx.moveTo(newKey);

    return retVal;
  }

  /**
   * Get node value of current node which is the string representation of {@link QName}s in the form
   * {@code prefix:localName} or the value of {@link TextNode}s.
   *
   * @param nodeKey node from which to get the value
   * @param rtx {@link XmlNodeReadOnlyTrx} implementation reference
   * @return string value of current node
   */
  String getNodeValue(final long nodeKey, final XmlNodeReadOnlyTrx rtx) {
    assert nodeKey >= 0;
    assert rtx != null;
    rtx.moveTo(nodeKey);
    final StringBuilder retVal = new StringBuilder();
    switch (rtx.getKind()) {
      case ELEMENT, NAMESPACE, ATTRIBUTE -> retVal.append(Utils.buildName(rtx.getName()));
      case TEXT, COMMENT -> retVal.append(rtx.getValue());
      case PROCESSING_INSTRUCTION -> retVal.append(rtx.getName().getLocalName()).append(" ").append(rtx.getValue());

      // $CASES-OMITTED$
      default -> {
      }
      // Do nothing.
    }
    return retVal.toString();
  }
}
