package org.sirix.page;

import org.sirix.page.interfaces.PageFragmentKey;

public final class PageFragmentKeyImpl implements PageFragmentKey {

  private final int revision;

  private final long key;

  public PageFragmentKeyImpl(final int revision, final long key) {
    this.revision = revision;
    this.key = key;
  }

  @Override
  public int getRevision() {
    return revision;
  }

  @Override
  public long getKey() {
    return key;
  }
}
