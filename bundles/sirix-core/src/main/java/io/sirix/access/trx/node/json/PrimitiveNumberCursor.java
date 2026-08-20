/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import io.sirix.access.trx.node.json.objectvalue.PrimitiveNumberValue;

/**
 * Internal cursor capability for copying an integral fused-number payload into caller-owned
 * primitive storage.
 *
 * <p>The value is consumed synchronously and no page-backed range escapes the cursor. A caller must
 * fall back to the ordinary {@code Number} API when {@link PrimitiveNumberValue#NONE} is returned;
 * that path preserves Double, Float, BigInteger, BigDecimal, and unknown future numeric kinds.</p>
 */
public interface PrimitiveNumberCursor {

  /**
   * Copies the current fused object's {@code int} or {@code long} payload into {@code valueOut}.
   * The output slot is left untouched when no unboxed integral representation is available.
   *
   * @param valueOut caller-owned primitive storage
   * @param index output index in {@code valueOut}
   * @return {@link PrimitiveNumberValue#INT}, {@link PrimitiveNumberValue#LONG}, or
   *         {@link PrimitiveNumberValue#NONE}
   */
  byte readFusedPrimitiveNumber(long[] valueOut, int index);
}
