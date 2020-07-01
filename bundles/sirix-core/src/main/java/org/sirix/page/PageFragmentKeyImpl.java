package org.sirix.page;

import org.sirix.page.interfaces.PageFragmentKey;

import static com.google.common.base.Preconditions.checkArgument;

public final class PageFragmentKeyImpl implements PageFragmentKey {

  private final int revision;

  private final long key;

  public PageFragmentKeyImpl(final int revision, final long key) {
    this.revision = revision;
    checkArgument(key >= 0);
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
