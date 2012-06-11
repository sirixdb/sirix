package org.sirix.gui.view.smallmultiple;

import org.sirix.gui.view.sunburst.SunburstItem;

public interface IFunction<R> {

  R apply(SunburstItem paramFirst, SunburstItem paramSecond, int paramIndexFirst, int paramIndexSecond);

}
