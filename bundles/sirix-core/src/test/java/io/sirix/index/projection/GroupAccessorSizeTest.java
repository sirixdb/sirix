/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.classfile.Attributes;
import java.lang.classfile.ClassFile;
import java.lang.classfile.MethodModel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Holds the method sizes a grouped row's cost depends on.
 *
 * <p>
 * Whether the lookup of an existing group is compiled into a row loop is decided by HotSpot's
 * inlining rules, and those are rules about bytecode size. Three of them carry the group table:
 * <ul>
 * <li>a callee of at most {@value #ALWAYS_INLINED} bytecodes ({@code -XX:MaxInlineSize}) is inlined
 * at every call site that was executed, however rarely: the lane accessors must be, because a
 * fold's seldom taken arm is still a row path once the data changes;
 * <li>a callee of at most {@value #NEVER_INLINED} bytecodes ({@code -XX:FreqInlineSize}) is inlined
 * where its call site is frequent: the probe and the record resolutions run once per row;
 * <li>a callee above that is never inlined: the insertion and the indexed acquisition must not be,
 * or the method that calls them grows with whatever they reach and stops being inlined itself.
 * </ul>
 * A refactoring that moves a method across one of these limits changes nothing a functional test
 * sees and costs a call per row, or per lane. It fails here instead.
 */
final class GroupAccessorSizeTest {

  /** {@code -XX:MaxInlineSize}: inlined at any executed call site. */
  private static final int ALWAYS_INLINED = 35;

  /**
   * {@code -XX:FreqInlineSize} on x86-64 and AArch64: the largest callee inlined at a hot call site.
   */
  private static final int NEVER_INLINED = 325;

  @Test
  void everyLaneAccessorIsInlinedAtAnyCallSite() throws IOException {
    final Map<String, Integer> kernel = sizes(GroupKernelLanes.class);
    for (final Map.Entry<String, Integer> method : kernel.entrySet()) {
      if (!method.getKey().startsWith("<")) {
        assertTrue(method.getValue() <= ALWAYS_INLINED,
            "GroupKernelLanes." + method.getKey() + " is " + method.getValue() + " bytecodes");
      }
    }
    final Map<String, Integer> checked = sizes(GroupLanes.class);
    for (final String method : List.of("get", "set", "add", "increment")) {
      assertTrue(size(checked, method) <= ALWAYS_INLINED, "GroupLanes." + method + " is " + size(checked, method));
    }
    assertTrue(size(sizes(GroupBuffer.class), "laneAddress") <= ALWAYS_INLINED);
  }

  @Test
  void theRowPathIsInlinableAndWhatItCallsOutToIsNot() throws IOException {
    final Map<String, Integer> table = sizes(NumericGroupAggTable.class);
    for (final String rowPath : List.of("acquire", "acquireExact", "probe", "probeExact", "acquireAccumulator",
        "accumulatorAt", "keyAtAccBase", "storageAtAccBase", "offsetAtAccBase")) {
      assertTrue(size(table, rowPath) <= NEVER_INLINED,
          "NumericGroupAggTable." + rowPath + " is " + size(table, rowPath) + " bytecodes: too large for a row loop");
    }
    for (final String outlined : List.of("insertInterleaved", "acquireIndexed")) {
      assertTrue(size(table, outlined) > NEVER_INLINED, "NumericGroupAggTable." + outlined + " is "
          + size(table, outlined) + " bytecodes: small enough to be compiled into acquire, which then is not inlined");
    }
    final Map<String, Integer> buffer = sizes(GroupBuffer.class);
    for (final String resolution : List.of("record", "accumulator")) {
      assertTrue(size(buffer, resolution) <= NEVER_INLINED);
    }
  }

  private static int size(final Map<String, Integer> sizes, final String method) {
    final Integer size = sizes.get(method);
    assertNotNull(size, "no method " + method);
    return size;
  }

  /** The largest code length per method name of {@code type}: overloads share a row path. */
  private static Map<String, Integer> sizes(final Class<?> type) throws IOException {
    final String resource = type.getSimpleName() + ".class";
    final byte[] bytes;
    try (InputStream in = type.getResourceAsStream(resource)) {
      assertNotNull(in, resource);
      bytes = in.readAllBytes();
    }
    final Map<String, Integer> sizes = new HashMap<>();
    for (final MethodModel method : ClassFile.of().parse(bytes).methods()) {
      method.findAttribute(Attributes.code())
            .ifPresent(code -> sizes.merge(method.methodName().stringValue(), code.codeLength(), Math::max));
    }
    assertFalse(sizes.isEmpty());
    return sizes;
  }
}
