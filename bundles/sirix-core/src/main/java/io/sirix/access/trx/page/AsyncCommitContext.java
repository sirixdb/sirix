/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.page;

import io.sirix.page.PageReference;
import io.sirix.settings.Constants;

import java.util.HashMap;
import java.util.Map;

/**
 * Thread-local context for async commit operations.
 * <p>
 * This class provides a way to override PageReference.getKey() values during
 * serialization without modifying the actual PageReference objects. This is
 * critical for async commits because the PageReference objects are shared
 * between the async commit thread and the main writer thread.
 * <p>
 * <b>Usage Pattern:</b>
 * <pre>{@code
 * try {
 *   AsyncCommitContext.begin();
 *   AsyncCommitContext.setKeyOverride(ref, diskOffset);
 *   // ... serialize pages ...
 * } finally {
 *   AsyncCommitContext.end();
 * }
 * }</pre>
 * <p>
 * The serialization code should call {@link #getKeyOverride(PageReference)}
 * to get the overridden key, falling back to {@link PageReference#getKey()}
 * if no override is set.
 *
 * @author Johannes Lichtenberger
 */
public final class AsyncCommitContext {

  /**
   * Thread-local map of PageReference identity hash codes to overridden disk offsets.
   * Using identity hash code instead of PageReference directly to avoid issues
   * with PageReference equality.
   */
  private static final ThreadLocal<Map<Integer, Long>> KEY_OVERRIDES = new ThreadLocal<>();

  /**
   * Thread-local map of PageReference identity hash codes to overridden hashes.
   * Used during async commit serialization to ensure correct hashes are written.
   */
  private static final ThreadLocal<Map<Integer, byte[]>> HASH_OVERRIDES = new ThreadLocal<>();

  private AsyncCommitContext() {
    // Utility class
  }

  /**
   * Begin an async commit context.
   * Must be paired with {@link #end()}.
   */
  public static void begin() {
    KEY_OVERRIDES.set(new HashMap<>());
    HASH_OVERRIDES.set(new HashMap<>());
  }

  /**
   * End an async commit context, clearing all overrides.
   */
  public static void end() {
    KEY_OVERRIDES.remove();
    HASH_OVERRIDES.remove();
  }

  /**
   * Set a disk offset override for a PageReference.
   * This override will be used by serialization code instead of ref.getKey().
   *
   * @param ref the PageReference
   * @param diskOffset the disk offset to use during serialization
   */
  public static void setKeyOverride(PageReference ref, long diskOffset) {
    Map<Integer, Long> map = KEY_OVERRIDES.get();
    if (map != null) {
      map.put(System.identityHashCode(ref), diskOffset);
    }
  }

  /**
   * Get the disk offset override for a PageReference, or the actual key if no override.
   * <p>
   * This method should be called by serialization code to get the correct disk offset.
   *
   * @param ref the PageReference
   * @return the overridden disk offset, or ref.getKey() if no override
   */
  public static long getKeyOrOverride(PageReference ref) {
    Map<Integer, Long> map = KEY_OVERRIDES.get();
    if (map != null) {
      Long override = map.get(System.identityHashCode(ref));
      if (override != null) {
        return override;
      }
    }
    return ref.getKey();
  }

  /**
   * Check if an async commit context is currently active.
   *
   * @return true if in an async commit context
   */
  public static boolean isActive() {
    return KEY_OVERRIDES.get() != null;
  }

  /**
   * Clear any override for a specific PageReference.
   *
   * @param ref the PageReference to clear
   */
  public static void clearKeyOverride(PageReference ref) {
    Map<Integer, Long> map = KEY_OVERRIDES.get();
    if (map != null) {
      map.remove(System.identityHashCode(ref));
    }
  }

  /**
   * Set a hash override for a PageReference.
   * This override will be used by serialization code instead of ref.getHash().
   *
   * @param ref the PageReference
   * @param hash the hash to use during serialization
   */
  public static void setHashOverride(PageReference ref, byte[] hash) {
    Map<Integer, byte[]> map = HASH_OVERRIDES.get();
    if (map != null) {
      map.put(System.identityHashCode(ref), hash);
    }
  }

  /**
   * Get the hash override for a PageReference, or the actual hash if no override.
   * <p>
   * This method should be called by serialization code to get the correct hash.
   *
   * @param ref the PageReference
   * @return the overridden hash, or ref.getHash() if no override
   */
  public static byte[] getHashOrOverride(PageReference ref) {
    Map<Integer, byte[]> map = HASH_OVERRIDES.get();
    if (map != null) {
      byte[] override = map.get(System.identityHashCode(ref));
      if (override != null) {
        return override;
      }
    }
    return ref.getHash();
  }

  /**
   * Clear any hash override for a specific PageReference.
   *
   * @param ref the PageReference to clear
   */
  public static void clearHashOverride(PageReference ref) {
    Map<Integer, byte[]> map = HASH_OVERRIDES.get();
    if (map != null) {
      map.remove(System.identityHashCode(ref));
    }
  }
}
