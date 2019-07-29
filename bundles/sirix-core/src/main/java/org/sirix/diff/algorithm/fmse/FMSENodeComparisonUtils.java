package org.sirix.diff.algorithm.fmse;

import javax.xml.namespace.QName;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.Utils;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.node.xdm.TextNode;

class FMSENodeComparisonUtils {

  /** Max length for Levenshtein comparsion. */
  private static final int MAX_LENGTH = 50;

  private final long mOldStartKey;

  private final long mNewStartKey;

  private final XmlNodeReadOnlyTrx mOldRtx;

  private final XmlNodeReadOnlyTrx mNewRtx;

  FMSENodeComparisonUtils(final long oldStartKey, final long newStartKey, final XmlNodeReadOnlyTrx oldRtx,
      final XmlNodeReadOnlyTrx newRtx) {
    mOldStartKey = oldStartKey;
    mNewStartKey = newStartKey;
    mOldRtx = oldRtx;
    mNewRtx = newRtx;
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

    return a == null
        ? b == null
        : a.equals(b);
  }

  /**
   * Calculate ratio between 0 and 1 between two String values for {@code text-nodes} denoted.
   *
   * @param pFirstNode node key of first node
   * @param pSecondNode node key of second node
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

    mOldRtx.moveTo(oldKey);
    mNewRtx.moveTo(newKey);

    boolean retVal = true;

    if (mOldRtx.hasParent() && mNewRtx.hasParent()) {
      do {
        mOldRtx.moveToParent();
        mNewRtx.moveToParent();

        if (mOldRtx.hasAttributes() && mNewRtx.hasAttributes() && !checkAttributes()) {
          return false;
        }
      } while (mOldRtx.getNodeKey() != mOldStartKey && mNewRtx.getNodeKey() != mNewStartKey && mOldRtx.hasParent()
          && mNewRtx.hasParent() && calculateRatio(getNodeValue(mOldRtx.getNodeKey(), mOldRtx),
              getNodeValue(mNewRtx.getNodeKey(), mNewRtx)) >= 0.7f);
      if ((mOldRtx.hasParent() && mOldRtx.getNodeKey() != mOldStartKey)
          || (mNewRtx.hasParent() && mNewRtx.getNodeKey() != mNewStartKey)) {
        retVal = false;
      } else {
        retVal = true;
      }
    } else {
      retVal = false;
    }

    mOldRtx.moveTo(oldKey);
    mNewRtx.moveTo(newKey);

    return retVal;
  }

  boolean checkAttributes() {
    final long newNodeKey = mNewRtx.getNodeKey();
    final long oldNodeKey = mOldRtx.getNodeKey();
    for (int i = 0, attCount = mOldRtx.getAttributeCount(); i < attCount; i++) {
      final QNm name = mOldRtx.moveToAttribute(i).trx().getName();

      if (mNewRtx.moveToAttributeByName(name).hasMoved() && (calculateRatio(getNodeValue(mOldRtx.getNodeKey(), mOldRtx),
          getNodeValue(mNewRtx.getNodeKey(), mNewRtx)) < 0.7f
          || calculateRatio(mOldRtx.getValue(), mNewRtx.getValue()) < 0.7f)) {
        mNewRtx.moveTo(newNodeKey);
        mOldRtx.moveTo(oldNodeKey);
        return false;
      }

      mNewRtx.moveTo(newNodeKey);
      mOldRtx.moveTo(oldNodeKey);
    }

    mNewRtx.moveTo(newNodeKey);
    mOldRtx.moveTo(oldNodeKey);
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
    mOldRtx.moveTo(oldKey);
    mNewRtx.moveTo(newKey);
    boolean retVal = false;
    if (mOldRtx.hasParent() && mNewRtx.hasParent()) {
      do {
        mOldRtx.moveToParent();
        mNewRtx.moveToParent();

        if (mOldRtx.isElement() && mNewRtx.isElement() && mOldRtx.moveToAttributeByName(id).hasMoved()
            && mNewRtx.moveToAttributeByName(id).hasMoved()) {
          if (mNewRtx.getValue().equals(mOldRtx.getValue()))
            retVal = true;
        } else {
          retVal = false;
          break;
        }
      } while (mOldRtx.getNodeKey() != mOldStartKey && mNewRtx.getNodeKey() != mNewStartKey && mOldRtx.hasParent()
          && mNewRtx.hasParent());
    }

    mOldRtx.moveTo(oldKey);
    mNewRtx.moveTo(newKey);

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
      case ELEMENT:
      case NAMESPACE:
      case ATTRIBUTE:
        retVal.append(Utils.buildName(rtx.getName()));
        break;
      case TEXT:
      case COMMENT:
        retVal.append(rtx.getValue());
        break;
      case PROCESSING_INSTRUCTION:
        retVal.append(rtx.getName().getLocalName()).append(" ").append(rtx.getValue());
        break;
      // $CASES-OMITTED$
      default:
        // Do nothing.
    }
    return retVal.toString();
  }
}
