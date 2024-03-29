package io.sirix.index;

import io.brackit.query.atomic.Bool;
import io.brackit.query.jdm.Type;
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