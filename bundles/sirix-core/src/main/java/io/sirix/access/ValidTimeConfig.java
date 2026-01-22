/*
 * Copyright (c) 2023, Sirix Contributors
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

package io.sirix.access;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Configuration for valid time support in bitemporal queries.
 *
 * <p>Valid time represents when data is/was/will be true in the real world,
 * as opposed to transaction time (when data was recorded in the database).</p>
 *
 * <p>This configuration specifies the JSON paths where valid time boundaries
 * are stored, enabling optimized bitemporal queries.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ValidTimeConfig {

  /**
   * JSON path to the validFrom field (start of valid time interval).
   */
  private final String validFromPath;

  /**
   * JSON path to the validTo field (end of valid time interval).
   */
  private final String validToPath;

  /**
   * Creates a new valid time configuration.
   *
   * @param validFromPath JSON path to the validFrom field (e.g., "$.validFrom" or "validFrom")
   * @param validToPath   JSON path to the validTo field (e.g., "$.validTo" or "validTo")
   * @throws NullPointerException if any argument is null
   */
  public ValidTimeConfig(String validFromPath, String validToPath) {
    this.validFromPath = requireNonNull(validFromPath, "validFromPath must not be null");
    this.validToPath = requireNonNull(validToPath, "validToPath must not be null");
  }

  /**
   * Creates a valid time configuration using convention-based field names.
   *
   * <p>This uses the standard field names "_validFrom" and "_validTo".</p>
   *
   * @return a new ValidTimeConfig using convention-based paths
   */
  public static ValidTimeConfig withConventionalPaths() {
    return new ValidTimeConfig("_validFrom", "_validTo");
  }

  /**
   * Creates a valid time configuration with custom paths.
   *
   * @param validFromPath JSON path to the validFrom field
   * @param validToPath   JSON path to the validTo field
   * @return a new ValidTimeConfig
   */
  public static ValidTimeConfig withPaths(String validFromPath, String validToPath) {
    return new ValidTimeConfig(validFromPath, validToPath);
  }

  /**
   * Gets the JSON path to the validFrom field.
   *
   * @return the validFrom path
   */
  public String getValidFromPath() {
    return validFromPath;
  }

  /**
   * Gets the JSON path to the validTo field.
   *
   * @return the validTo path
   */
  public String getValidToPath() {
    return validToPath;
  }

  /**
   * Normalizes a path by removing the "$." prefix if present.
   *
   * @param path the path to normalize
   * @return the normalized path
   */
  public static String normalizePath(String path) {
    if (path.startsWith("$.")) {
      return path.substring(2);
    }
    return path;
  }

  /**
   * Gets the normalized validFrom path (without "$." prefix).
   *
   * @return the normalized validFrom path
   */
  public String getNormalizedValidFromPath() {
    return normalizePath(validFromPath);
  }

  /**
   * Gets the normalized validTo path (without "$." prefix).
   *
   * @return the normalized validTo path
   */
  public String getNormalizedValidToPath() {
    return normalizePath(validToPath);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ValidTimeConfig that)) return false;
    return Objects.equals(validFromPath, that.validFromPath)
        && Objects.equals(validToPath, that.validToPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(validFromPath, validToPath);
  }

  @Override
  public String toString() {
    return "ValidTimeConfig{" +
        "validFromPath='" + validFromPath + '\'' +
        ", validToPath='" + validToPath + '\'' +
        '}';
  }
}
