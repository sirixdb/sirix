/*
 * Copyright (c) 2024, SirixDB
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
package io.sirix.io;

import io.sirix.access.ResourceConfiguration;

/**
 * Service Provider Interface (SPI) for pluggable storage backends.
 *
 * <p>
 * Enterprise or third-party storage implementations can register themselves via Java's
 * {@link java.util.ServiceLoader} mechanism by:
 * <ol>
 * <li>Implementing this interface</li>
 * <li>Creating a file {@code META-INF/services/io.sirix.io.StorageProvider}</li>
 * <li>Listing the fully-qualified implementation class name in that file</li>
 * </ol>
 *
 * <p>
 * Example usage in sirix-enterprise:
 * 
 * <pre>
 * {
 *   &#64;code
 *   public class FFMIOUringStorageProvider implements StorageProvider {
 *     &#64;Override
 *     public String getName() {
 *       return "IO_URING_FFM";
 *     }
 *
 *     &#64;Override
 *     public boolean isAvailable() {
 *       return FFIIOUring.isAvailable();
 *     }
 *
 *     @Override
 *     public IOStorage createStorage(ResourceConfiguration config) {
 *       return new FFMIOUringStorage(config);
 *     }
 *   }
 * }
 * </pre>
 *
 * @author Johannes Lichtenberger
 * @since 2.0.0
 */
public interface StorageProvider {

  /**
   * Get the unique name of this storage provider.
   *
   * <p>
   * This name is used to select the provider via configuration. Convention: uppercase with
   * underscores (e.g., "IO_URING_FFM", "SPDK").
   *
   * @return the provider name, never null
   */
  String getName();

  /**
   * Check if this storage provider is available on the current system.
   *
   * <p>
   * Implementations should check for:
   * <ul>
   * <li>Required native libraries (e.g., liburing for io_uring)</li>
   * <li>OS compatibility (e.g., Linux-only features)</li>
   * <li>Hardware requirements</li>
   * <li>License validation for commercial providers</li>
   * </ul>
   *
   * @return true if this provider can be used, false otherwise
   */
  boolean isAvailable();

  /**
   * Get a human-readable description of why this provider is unavailable.
   *
   * <p>
   * Only called when {@link #isAvailable()} returns false.
   *
   * @return description of unavailability reason, or null if available
   */
  default String getUnavailabilityReason() {
    return null;
  }

  /**
   * Create a storage instance for the given resource configuration.
   *
   * @param resourceConfig the resource configuration
   * @return a new IOStorage instance
   * @throws IllegalStateException if provider is not available
   */
  IOStorage createStorage(ResourceConfiguration resourceConfig);

  /**
   * Get the priority of this provider.
   *
   * <p>
   * When multiple providers with the same name are available, the one with the highest priority wins.
   * Default is 0. Enterprise providers should use higher priorities (e.g., 100).
   *
   * @return the provider priority
   */
  default int getPriority() {
    return 0;
  }

  /**
   * Check if this is an enterprise/commercial provider.
   *
   * <p>
   * Used for logging and diagnostics.
   *
   * @return true if this is an enterprise feature
   */
  default boolean isEnterprise() {
    return false;
  }
}
