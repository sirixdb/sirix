/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.HashMap;
import java.util.Map;

/**
 * Debug mode's records ({@link GroupKernelLanes#DEBUG}): the bounded views that stand in for the
 * addresses the aggregation kernels use in production.
 *
 * <p>
 * Resolving a record in debug mode cuts a view of exactly the resolved footprint from its
 * allocation and files it here; the record the kernel receives is that filing's number, not an
 * address. Each lane access looks the view up again and goes through its bounded segment, so a lane
 * outside the footprint fails even when it lies inside the same chunk (the neighbouring stripe),
 * and a view of a chunk that has been handed back fails even after another table took the same
 * memory: the number names one footprint of one generation of one allocation, for as long as it is
 * used.
 *
 * <p>
 * The filings are per thread, because a record is used by the thread that resolved it, and the same
 * footprint resolved again answers the same number, so a row loop that resolves per row files once
 * per group. Filings of allocations that are gone are dropped when the file has grown.
 */
final class GroupDebugRecords {

  /** Marks a record as a filing number: never a valid address, in either mode. */
  private static final long TAG = Long.MIN_VALUE;

  /** Filings beyond which the dead ones are looked for. */
  private static final int SWEEP_AT = 1 << 16;

  // Debug filing identities live with their thread; dead allocations are swept from the maps.
  @SuppressWarnings("java:S5164")
  private static final ThreadLocal<GroupDebugRecords> OF_THREAD = ThreadLocal.withInitial(GroupDebugRecords::new);

  /** One footprint of one allocation; the allocation compares by identity. */
  private record Footprint(GroupBuffer allocation, int first, int lanes) {
  }

  private final Map<Footprint, Long> numbers = new HashMap<>();
  private final Map<Long, GroupBuffer> views = new HashMap<>();
  private long filed;
  private int sweepAt = SWEEP_AT;

  private GroupDebugRecords() {}

  /**
   * The record of the lanes {@code [first, first + lanes)} of {@code source}, whose range and
   * liveness the caller has checked.
   */
  static long open(final GroupBuffer source, final int first, final int lanes) {
    final GroupDebugRecords file = OF_THREAD.get();
    final Footprint footprint = new Footprint(source.allocation(), first, lanes);
    final Long known = file.numbers.get(footprint);
    if (known != null) {
      return known;
    }
    if (file.views.size() >= file.sweepAt) {
      file.sweep();
    }
    final long record = TAG | file.filed++;
    file.numbers.put(footprint, record);
    file.views.put(record, source.view(first, lanes));
    return record;
  }

  static long get(final long record, final int lane) {
    final GroupBuffer view = view(record);
    return GroupLanes.get(view, view.firstLane() + lane(view, lane));
  }

  static void set(final long record, final int lane, final long value) {
    final GroupBuffer view = view(record);
    GroupLanes.set(view, view.firstLane() + lane(view, lane), value);
  }

  static long getOpaque(final long record, final int lane) {
    final GroupBuffer view = view(record);
    return GroupLanes.boundedGetOpaque(view, lane(view, lane));
  }

  static long compareAndExchange(final long record, final int lane, final long expected, final long value) {
    final GroupBuffer view = view(record);
    return GroupLanes.boundedCompareAndExchange(view, lane(view, lane), expected, value);
  }

  static long getAndAdd(final long record, final int lane, final long value) {
    final GroupBuffer view = view(record);
    return GroupLanes.boundedGetAndAdd(view, lane(view, lane), value);
  }

  static void add(final long record, final int lane, final long delta) {
    set(record, lane, get(record, lane) + delta);
  }

  static void min(final long record, final int lane, final long value) {
    if (value < get(record, lane)) {
      set(record, lane, value);
    }
  }

  static void max(final long record, final int lane, final long value) {
    if (value > get(record, lane)) {
      set(record, lane, value);
    }
  }

  private static GroupBuffer view(final long record) {
    final GroupBuffer view = record < 0L
        ? OF_THREAD.get().views.get(record)
        : null;
    if (view == null) {
      throw new IllegalStateException("not an aggregation record this thread resolved");
    }
    return view;
  }

  /**
   * {@code lane} unchanged when it lies in the view; the view's own range check words the failure.
   */
  private static int lane(final GroupBuffer view, final int lane) {
    if (lane < 0 || lane >= view.viewLanes()) {
      throw new IndexOutOfBoundsException("lane " + lane + " outside a record of " + view.viewLanes() + " lanes");
    }
    return lane;
  }

  /** Drop the filings whose allocation was handed back or whose workspace is closed. */
  private void sweep() {
    numbers.entrySet().removeIf(filing -> {
      if (views.get(filing.getValue()).isLive()) {
        return false;
      }
      views.remove(filing.getValue());
      return true;
    });
    sweepAt = Math.max(SWEEP_AT, views.size() << 1);
  }
}
