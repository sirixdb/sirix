/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.service.json.serialize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * XMLSerializer properties.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class JsonSerializerProperties {

  /** Logger. */
  private static final LogWrapper LOGGER = new LogWrapper(LoggerFactory.getLogger(JsonSerializerProperties.class));

  // ============== Class constants. =================

  /** NO maps to false. */
  private static final boolean NO = false;

  // ============ Shredding constants. ===============

  /** Serialization parameter: yes/no. */
  public static final Object[] S_INDENT = {"indent", NO};

  /** Specific serialization parameter: number of spaces to indent. */
  public static final Object[] S_INDENT_SPACES = {"indent-spaces", 2};

  /** Property file. */
  private String mFilePath;

  /** Properties. */
  private final ConcurrentMap<String, Object> mProps = new ConcurrentHashMap<String, Object>();

  /**
   * Constructor.
   */
  public JsonSerializerProperties() {
    try {
      for (final Field f : getClass().getFields()) {
        final Object obj = f.get(null);
        if (!(obj instanceof Object[])) {
          continue;
        }
        final Object[] arr = (Object[]) obj;
        mProps.put(arr[0].toString(), arr[1]);
      }
    } catch (final IllegalArgumentException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * <p>
   * Read properties file into a concurrent map. Format of properties file:
   * </p>
   *
   * <ul>
   * <li>xmldecl=yes (possible values: yes/no)</li>
   * <li>indent=no (possible values: yes/no)</li>
   * <li>indent-spaces=2 (possible values: Integer)</li>
   * <li>serialize-rest=no (possible values: yes/no)</li>
   * <li>serialize-id=no (possible values: yes/no)</li>
   * </ul>
   *
   * <p>
   * Note that currently all properties have to be set. If specific key/value pairs are specified more
   * than once the last values are preserved, so the default values are overridden by user specified
   * values.
   * </p>
   *
   * @param filePath path to properties file
   * @return ConcurrentMap which holds property key/values.
   */
  public ConcurrentMap<String, Object> readProps(final String filePath) {
    mFilePath = filePath;
    if (!new File(mFilePath).exists()) {
      throw new IllegalStateException("Properties file doesn't exist!");
    }

    try {
      // Read and parse file.
      try (BufferedReader buffReader = new BufferedReader(new FileReader(mFilePath))) {
        for (String line = buffReader.readLine(); line != null; line = buffReader.readLine()) {
          line = line.trim();
          if (line.isEmpty()) {
            continue;
          }

          final int equals = line.indexOf('=');
          if (equals < 0) {
            LOGGER.error("Properties file has no '=' sign in line -- parsing error!");
          }

          final String key = line.substring(0, equals).toUpperCase();
          final Object value = line.substring(equals + 1);

          mProps.put(key, value);
        }
      }
    } catch (final IOException e) {
      LOGGER.error(e.getMessage(), e);
    }

    return mProps;
  }

  /**
   * Get properties map.
   *
   * @return ConcurrentMap with key/value property pairs.
   */
  public ConcurrentMap<String, Object> getProps() {
    return mProps;
  }

}
