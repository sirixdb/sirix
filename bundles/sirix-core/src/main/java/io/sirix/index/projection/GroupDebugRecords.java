/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Debug mode's records ({@link GroupKernelLanes#DEBUG}): the bounded views that stand in for the
 * addresses the aggregation kernels use in production.
 *
 * <p>
 * Resolving a record in debug mode files the resolved footprint of its allocation here; the record
 * the kernel receives is that filing's number, not an address. Each lane access looks the footprint
 * up again, checks its bounds and goes through the allocation's bounded segment, so a lane
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

  private record Footprint(int first, int lanes) {
  }

  private record View(WeakReference<GroupBuffer> allocation, int first, int lanes) {
    private View(final GroupBuffer bounded) {
      this(new WeakReference<>(bounded.allocation()), bounded.firstLane(), bounded.viewLanes());
    }

    private GroupBuffer buffer() {
      final GroupBuffer buffer = allocation.get();
      if (buffer == null) {
        throw new IllegalStateException("aggregation allocation is no longer reachable");
      }
      return buffer;
    }

    private boolean isLive() {
      final GroupBuffer buffer = allocation.get();
      return buffer != null && buffer.isLive();
    }
  }

  private final Map<GroupBuffer, Map<Footprint, Long>> numbers = new WeakHashMap<>();
  private final Map<Long, View> views = new HashMap<>();
  private long filed;
  private int sweepAt = SWEEP_AT;

  private GroupDebugRecords() {}

  /**
   * The record of the lanes {@code [first, first + lanes)} of {@code source}, whose range and
   * liveness the caller has checked.
   */
  static long open(final GroupBuffer source, final int first, final int lanes) {
    final GroupDebugRecords file = OF_THREAD.get();
    if (file.views.size() >= file.sweepAt) {
      file.sweep();
    }
    final Map<Footprint, Long> footprints = file.numbers.computeIfAbsent(source.allocation(), _ -> new HashMap<>());
    final Footprint footprint = new Footprint(first, lanes);
    final Long known = footprints.get(footprint);
    if (known != null) {
      return known;
    }
    final long groupRecord = TAG | file.filed++;
    footprints.put(footprint, groupRecord);
    file.views.put(groupRecord, new View(source.view(first, lanes)));
    return groupRecord;
  }

  static long get(final long groupRecord, final int lane) {
    final View view = view(groupRecord);
    return GroupLanes.get(view.buffer(), lane(view, lane));
  }

  static void set(final long groupRecord, final int lane, final long value) {
    final View view = view(groupRecord);
    GroupLanes.set(view.buffer(), lane(view, lane), value);
  }

  static long getOpaque(final long groupRecord, final int lane) {
    final View view = view(groupRecord);
    return GroupLanes.boundedGetOpaque(view.buffer(), lane(view, lane));
  }

  static long compareAndExchange(final long groupRecord, final int lane, final long expected, final long value) {
    final View view = view(groupRecord);
    return GroupLanes.boundedCompareAndExchange(view.buffer(), lane(view, lane), expected, value);
  }

  static long getAndAdd(final long groupRecord, final int lane, final long value) {
    final View view = view(groupRecord);
    return GroupLanes.boundedGetAndAdd(view.buffer(), lane(view, lane), value);
  }

  static void add(final long groupRecord, final int lane, final long delta) {
    set(groupRecord, lane, get(groupRecord, lane) + delta);
  }

  static void min(final long groupRecord, final int lane, final long value) {
    if (value < get(groupRecord, lane)) {
      set(groupRecord, lane, value);
    }
  }

  static void max(final long groupRecord, final int lane, final long value) {
    if (value > get(groupRecord, lane)) {
      set(groupRecord, lane, value);
    }
  }

  private static View view(final long groupRecord) {
    final View view = groupRecord < 0L
        ? OF_THREAD.get().views.get(groupRecord)
        : null;
    if (view == null) {
      throw new IllegalStateException("not an aggregation record this thread resolved");
    }
    return view;
  }

  private static int lane(final View view, final int lane) {
    if (lane < 0 || lane >= view.lanes()) {
      throw new IndexOutOfBoundsException("lane " + lane + " outside a record of " + view.lanes() + " lanes");
    }
    return view.first() + lane;
  }

  /** Drop the filings whose allocation was handed back or whose workspace is closed. */
  private void sweep() {
    views.values().removeIf(view -> !view.isLive());
    numbers.values().removeIf(footprints -> {
      footprints.values().removeIf(number -> !views.containsKey(number));
      return footprints.isEmpty();
    });
    sweepAt = Math.max(SWEEP_AT, views.size() << 1);
  }
}
