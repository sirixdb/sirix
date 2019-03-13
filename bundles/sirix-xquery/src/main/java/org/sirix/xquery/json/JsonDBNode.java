package org.sirix.xquery.json;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.Array;
import org.brackit.xquery.xdm.json.Record;
import org.brackit.xquery.xdm.json.TemporalJsonItem;
import org.brackit.xquery.xdm.type.ItemType;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.utils.LogWrapper;
import org.sirix.xquery.StructuredDBItem;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;

public final class JsonDBNode extends AbstractItem
    implements TemporalJsonItem, Array, Record, StructuredDBItem<JsonNodeReadOnlyTrx> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(JsonDBNode.class));

  /** Sirix {@link v}. */
  private final JsonNodeReadOnlyTrx mRtx;

  /** Sirix node key. */
  private final long mNodeKey;

  /** Kind of node. */
  private final org.sirix.node.Kind mKind;

  /** Collection this node is part of. */
  private final JsonDBCollection mCollection;

  /** Determines if write-transaction is present. */
  private final boolean mIsWtx;


  /**
   * Constructor.
   *
   * @param rtx {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   */
  public JsonDBNode(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection) {
    mCollection = Preconditions.checkNotNull(collection);
    mRtx = Preconditions.checkNotNull(rtx);
    mIsWtx = mRtx instanceof JsonNodeTrx;
    mNodeKey = mRtx.getNodeKey();
    mKind = mRtx.getKind();
  }

  public JsonDBCollection getCollection() {
    return mCollection;
  }

  @Override
  public TemporalJsonItem getNext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TemporalJsonItem getPrevious() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TemporalJsonItem getFirst() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TemporalJsonItem getLast() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stream<TemporalJsonItem> getEarlier(boolean includeSelf) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stream<TemporalJsonItem> getFuture(boolean includeSelf) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Stream<TemporalJsonItem> getAllTime() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isNextOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isPreviousOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isFutureOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isFutureOrSelfOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isEarlierOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isEarlierOrSelfOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLastOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isFirstOf(TemporalJsonItem other) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ItemType itemType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Atomic atomize() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean booleanValue() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Sequence get(QNm field) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Sequence value(IntNumeric i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Sequence value(int i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array names() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array values() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QNm name(IntNumeric i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public QNm name(int i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Sequence at(IntNumeric i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Sequence at(int i) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IntNumeric length() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int len() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Array range(IntNumeric from, IntNumeric to) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JsonNodeReadOnlyTrx getTrx() {
    return mRtx;
  }
}
