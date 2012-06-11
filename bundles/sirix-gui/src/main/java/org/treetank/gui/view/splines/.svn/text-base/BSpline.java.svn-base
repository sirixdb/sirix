package org.treetank.gui.view.splines;

import java.util.LinkedList;
import java.util.List;

import org.treetank.gui.view.sunburst.AbsSunburstGUI;
import org.treetank.gui.view.sunburst.AbsSunburstGUI.EDrawLine;
import processing.core.PGraphics;
import processing.core.PVector;

public class BSpline implements ISpline {

  // The basis function for a cubic B spline.
  @Override
  public float b(int i, float t) {
    switch (i) {
    case -2:
      return (((-t + 3) * t - 3) * t + 1) / 6;
    case -1:
      return (((3 * t - 6) * t) * t + 4) / 6;
    case 0:
      return (((-3 * t + 3) * t + 3) * t + 1) / 6;
    case 1:
      return (t * t * t) / 6;
    }
    return 0; // we only get here if an invalid i is specified
  }

  // evaluate a point on the B spline
  @Override
  public PVector p(int i, float t, final List<PVector> pPath) {
    float px = 0;
    float py = 0;
    for (int j = -2; j <= 1 && i + j < pPath.size(); j++) {
      px += b(j, t) * pPath.get(i + j).x;
      py += b(j, t) * pPath.get(i + j).y;
    }
    return new PVector(px, py);
  }

  private final int STEPS = 12;

  @Override
  public void draw(final AbsSunburstGUI pGUI, final PGraphics pGraphic, final List<PVector> pPath) {
    final List<PVector> path = new LinkedList<>();
    PVector q = new PVector(pPath.get(0).x, pPath.get(0).y);
    path.add(q);
    for (int i = 2; i < pPath.size() - 1; i++) {
      for (int j = 1; j <= STEPS; j++) {
        q = p(i, j / (float)STEPS, pPath);
        path.add(q);
      }
    }

    pGraphic.beginShape();
    pGraphic.vertex(path.get(0).x, path.get(0).y);
    int i = 0;
    for (; i < path.size() - 3; i += 3) {
      pGraphic.bezierVertex(path.get(i).x, path.get(i).y, path.get(i + 1).x, path.get(i + 1).y, path
        .get(i + 2).x, path.get(i + 2).y);
    }

    final PVector end = new PVector(pPath.get(pPath.size() - 1).x, pPath.get(pPath.size() - 1).y);
    if (i - 1 > 0 && i < path.size()) {
      pGraphic.bezierVertex(path.get(i - 1).x, path.get(i - 1).y, path.get(i).x, path.get(i).y, end.x, end.y);
      pGUI.drawArrow(pGraphic, path.get(i).x, path.get(i).y, end.x, end.y, EDrawLine.NO);
    } else {
      pGUI.drawArrow(pGraphic, q.x, q.y, end.x, end.y, EDrawLine.YES);
    }
    pGraphic.endShape();
  }
}
