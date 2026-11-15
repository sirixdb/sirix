/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * The {@code VarHandle}s of the scratch-buffer borrow path, and nothing else.
 *
 * <p>
 * A native image compiles a handle access to a plain load, store or compare-and-set only where the
 * handle is a constant, and it folds only the static finals of classes initialized at build time; a
 * handle held by a class the image initializes at run time is invoked through the generic
 * method-handle path on every access. Whether a class qualifies is decided by the builder's
 * simulation of its initializer, which a logger or a property read silently turns off. So these
 * handles live in a class that holds only handles, {@code native-image.properties} lists it for
 * build-time initialization, and every access site names this class directly.
 */
final class ScratchHandles {

  /** {@link ScratchCarrier#era}: the ownership protocol of one buffer. */
  static final VarHandle ERA;

  /** {@link ScratchCarrier#acquisitions}. */
  static final VarHandle ACQUISITIONS;

  /** {@link ScratchCarrier#requestedBytes}. */
  static final VarHandle REQUESTED_BYTES;

  /** An element of a carrier's per-use counter arrays. */
  static final VarHandle COUNTER = MethodHandles.arrayElementVarHandle(long[].class);

  /** A slot of an idle-buffer array. */
  static final VarHandle IDLE_SLOT = MethodHandles.arrayElementVarHandle(ScratchCarrier[].class);

  /** {@link ScratchLease#carrier}: taken exactly once by a close. */
  static final VarHandle LEASE_CARRIER;

  static {
    try {
      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      ERA = lookup.findVarHandle(ScratchCarrier.class, "era", long.class);
      ACQUISITIONS = lookup.findVarHandle(ScratchCarrier.class, "acquisitions", long.class);
      REQUESTED_BYTES = lookup.findVarHandle(ScratchCarrier.class, "requestedBytes", long.class);
      LEASE_CARRIER = lookup.findVarHandle(ScratchLease.class, "carrier", ScratchCarrier.class);
    } catch (final ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private ScratchHandles() {}
}
