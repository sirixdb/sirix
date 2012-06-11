package org.sirix.gui.view.smallmultiple;

import static com.google.common.base.Preconditions.checkArgument;

class Indexes {

  int mFirstIndex;

  int mSecondIndex;

  Indexes(final int paramFirstIndex, final int paramSecondIndex) {
    checkArgument(paramFirstIndex >= 0);
    checkArgument(paramSecondIndex >= 0);
    mFirstIndex = paramFirstIndex;
    mSecondIndex = paramSecondIndex;
  }
}
