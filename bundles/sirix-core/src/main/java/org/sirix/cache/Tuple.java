package org.sirix.cache;

import com.google.common.base.Objects;
import org.sirix.page.EPage;

public class Tuple {
  private long mKey;
  private EPage mPage;

  public Tuple(final long pKey, final EPage pPage) {
    mKey = pKey;
    mPage = pPage;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mKey, mPage);
  }
  
  @Override
  public boolean equals(final Object pObj) {
    if (pObj instanceof Tuple) {
      final Tuple otherTuple = (Tuple)pObj;
      return Objects.equal(mKey, otherTuple.mKey)
        && Objects.equal(mPage, otherTuple.mPage);
    }
    return false;
  }
  
  public long getKey() {
    return mKey;
  }
  
  public EPage getPage() {
    return mPage;
  }
}
