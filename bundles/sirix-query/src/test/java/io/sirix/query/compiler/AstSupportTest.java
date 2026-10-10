package io.sirix.query.compiler;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class AstSupportTest {
  @Test
  void copiesOwnNestedArrayAnnotationsAndValues() {
    final AST source = new AST(XQ.MainModule, new int[] {7});
    source.setProperty("columns", new int[][] {{1, 2}, {3}});
    final AST child = new AST(XQ.Int);
    child.setProperty("order", new boolean[] {true, false});
    source.addChild(child);
    source.addChild(new AST(XQ.Int, "plain node without annotations"));
    final AST first = AstSupport.copyTree(source);
    final AST second = AstSupport.copyTree(source);
    assertNotSame(source.getProperties(), first.getProperties());
    assertNotSame(first.getProperty("columns"), second.getProperty("columns"));
    ((int[]) first.getValue())[0] = 99;
    ((int[][]) first.getProperty("columns"))[0][0] = 99;
    ((boolean[]) first.getChild(0).getProperty("order"))[0] = false;
    assertArrayEquals(new int[] {7}, (int[]) source.getValue());
    assertArrayEquals(new int[] {1, 2}, ((int[][]) source.getProperty("columns"))[0]);
    assertArrayEquals(new int[] {1, 2}, ((int[][]) second.getProperty("columns"))[0]);
    assertArrayEquals(new boolean[] {true, false}, (boolean[]) child.getProperty("order"));
    assertArrayEquals(new boolean[] {true, false}, (boolean[]) second.getChild(0).getProperty("order"));
  }

  @Test
  void diagnosticsRenderBooleanContentsWithoutReplacingArrayAnnotations() {
    final AST source = new AST(XQ.MainModule);
    source.setProperty("order", new boolean[] {true, false});
    final String json = AstSupport.toJSON(source);
    assertTrue(json.contains("[true, false]"));
    assertFalse(json.contains("[Z@"));
    assertArrayEquals(new boolean[] {true, false}, (boolean[]) source.getProperty("order"));
  }
}
