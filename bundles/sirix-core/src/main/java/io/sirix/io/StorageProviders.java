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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Registry and factory for storage providers discovered via ServiceLoader.
 *
 * <p>This class discovers {@link StorageProvider} implementations at runtime,
 * allowing enterprise or third-party storage backends to be plugged in without
 * modifying sirix-core.
 *
 * <p>Providers are discovered once at class load time and cached. When multiple
 * providers register the same name, the one with the highest priority wins.
 *
 * @author Johannes Lichtenberger
 * @since 2.0.0
 */
public final class StorageProviders {

  private static final Logger LOGGER = LoggerFactory.getLogger(StorageProviders.class);

  /**
   * Cache of discovered providers, keyed by name.
   * When multiple providers have the same name, highest priority wins.
   */
  private static final Map<String, StorageProvider> PROVIDERS = new ConcurrentHashMap<>();

  /**
   * All discovered providers for listing/diagnostics.
   */
  private static final List<StorageProvider> ALL_PROVIDERS;

  static {
    // Discover all providers via ServiceLoader
    ServiceLoader<StorageProvider> loader = ServiceLoader.load(StorageProvider.class);

    ALL_PROVIDERS = loader.stream()
        .map(ServiceLoader.Provider::get)
        .sorted(Comparator.comparingInt(StorageProvider::getPriority).reversed())
        .collect(Collectors.toList());

    // Register providers, highest priority wins for each name
    for (StorageProvider provider : ALL_PROVIDERS) {
      String name = provider.getName().toUpperCase();
      StorageProvider existing = PROVIDERS.get(name);

      if (existing == null || provider.getPriority() > existing.getPriority()) {
        PROVIDERS.put(name, provider);

        if (provider.isAvailable()) {
          LOGGER.info("Registered storage provider: {} (priority={}, enterprise={})",
              name, provider.getPriority(), provider.isEnterprise());
        } else {
          LOGGER.debug("Storage provider {} registered but unavailable: {}",
              name, provider.getUnavailabilityReason());
        }
      } else {
        LOGGER.debug("Skipping provider {} with lower priority ({} < {})",
            provider.getClass().getName(), provider.getPriority(), existing.getPriority());
      }
    }

    if (PROVIDERS.isEmpty()) {
      LOGGER.debug("No external storage providers discovered via ServiceLoader");
    } else {
      LOGGER.info("Discovered {} storage provider(s): {}",
          PROVIDERS.size(), PROVIDERS.keySet());
    }
  }

  private StorageProviders() {
    // Utility class
  }

  /**
   * Get a storage provider by name.
   *
   * @param name the provider name (case-insensitive)
   * @return the provider, or empty if not found
   */
  public static Optional<StorageProvider> get(String name) {
    return Optional.ofNullable(PROVIDERS.get(name.toUpperCase()));
  }

  /**
   * Check if a provider with the given name exists and is available.
   *
   * @param name the provider name (case-insensitive)
   * @return true if provider exists and is available
   */
  public static boolean isAvailable(String name) {
    return get(name).map(StorageProvider::isAvailable).orElse(false);
  }

  /**
   * Create storage using a named provider.
   *
   * @param name the provider name
   * @param resourceConfig the resource configuration
   * @return the storage instance
   * @throws IllegalArgumentException if provider not found
   * @throws IllegalStateException if provider not available
   */
  public static IOStorage createStorage(String name, ResourceConfiguration resourceConfig) {
    StorageProvider provider = get(name)
        .orElseThrow(() -> new IllegalArgumentException("Unknown storage provider: " + name));

    if (!provider.isAvailable()) {
      throw new IllegalStateException("Storage provider " + name + " is not available: "
          + provider.getUnavailabilityReason());
    }

    LOGGER.debug("Creating storage via provider: {} (enterprise={})",
        name, provider.isEnterprise());

    return provider.createStorage(resourceConfig);
  }

  /**
   * Get all registered provider names.
   *
   * @return list of provider names
   */
  public static List<String> getProviderNames() {
    return List.copyOf(PROVIDERS.keySet());
  }

  /**
   * Get all available provider names (those that can actually be used).
   *
   * @return list of available provider names
   */
  public static List<String> getAvailableProviderNames() {
    return PROVIDERS.entrySet().stream()
        .filter(e -> e.getValue().isAvailable())
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Get all discovered providers for diagnostics.
   *
   * @return unmodifiable list of all providers
   */
  public static List<StorageProvider> getAllProviders() {
    return List.copyOf(ALL_PROVIDERS);
  }

  /**
   * Check if any enterprise providers are available.
   *
   * @return true if at least one enterprise provider is available
   */
  public static boolean hasEnterpriseProviders() {
    return ALL_PROVIDERS.stream()
        .anyMatch(p -> p.isEnterprise() && p.isAvailable());
  }

  /**
   * Print diagnostic information about all providers.
   */
  public static void printDiagnostics() {
    LOGGER.info("=== Storage Provider Diagnostics ===");
    LOGGER.info("Total providers discovered: {}", ALL_PROVIDERS.size());

    for (StorageProvider provider : ALL_PROVIDERS) {
      LOGGER.info("  {} [priority={}, enterprise={}, available={}]{}",
          provider.getName(),
          provider.getPriority(),
          provider.isEnterprise(),
          provider.isAvailable(),
          provider.isAvailable() ? "" : " - " + provider.getUnavailabilityReason());
    }

    LOGGER.info("====================================");
  }
}
