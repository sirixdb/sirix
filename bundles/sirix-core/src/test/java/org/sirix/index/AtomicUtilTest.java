package org.sirix.index;

import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.jdm.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class AtomicUtilTest {
  @Test
  public void testBooleanTrue() {
    final byte[] bytes = AtomicUtil.toBytes(new Bool(true));
    assertEquals(new Bool(true), AtomicUtil.fromBytes(bytes, Type.BOOL));
  }

  @Test
  public void testBooleanFalse() {
    final byte[] bytes = AtomicUtil.toBytes(new Bool(false));
    assertEquals(new Bool(false), AtomicUtil.fromBytes(bytes, Type.BOOL));
  }
}