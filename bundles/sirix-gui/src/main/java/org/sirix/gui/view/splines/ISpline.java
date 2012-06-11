package org.sirix.gui.view.splines;

import java.util.List;

import org.sirix.gui.view.sunburst.AbsSunburstGUI;

import processing.core.PGraphics;
import processing.core.PVector;

public interface ISpline {
  float b(int i, float t);

  PVector p(int i, float t, final List<PVector> paramPath);

  void draw(final AbsSunburstGUI pGUI, final PGraphics paramGraphic, final List<PVector> paramPath);
}
