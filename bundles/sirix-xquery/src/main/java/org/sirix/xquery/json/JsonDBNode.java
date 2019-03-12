package org.sirix.xquery.json;

import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.xdm.AbstractItem;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.json.TemporalJsonItem;
import org.brackit.xquery.xdm.type.ItemType;

public final class JsonDBNode extends AbstractItem implements TemporalJsonItem {


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
}
