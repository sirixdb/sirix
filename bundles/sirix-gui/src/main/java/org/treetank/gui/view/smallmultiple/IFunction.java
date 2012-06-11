package org.treetank.gui.view.smallmultiple;

import org.treetank.gui.view.sunburst.SunburstItem;

public interface IFunction<R> {

  R apply(SunburstItem paramFirst, SunburstItem paramSecond, int paramIndexFirst, int paramIndexSecond);

}
