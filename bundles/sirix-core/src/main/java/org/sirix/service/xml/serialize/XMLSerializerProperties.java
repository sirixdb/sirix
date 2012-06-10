/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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

package org.sirix.service.xml.serialize;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * <h1>XMLSerializerProperties</h1>
 * 
 * <p>
 * XMLSerializer properties.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XMLSerializerProperties {

  // ============== Class constants. =================

  /** YES maps to true. */
  private static final boolean YES = true;

  /** NO maps to false. */
  private static final boolean NO = false;

  // ============ Shredding constants. ===============

  /** Serialize TT-ID: yes/no. */
  public static final Object[] S_ID = {
    "serialize-id", NO
  };

  /** Serialization parameter: yes/no. */
  public static final Object[] S_INDENT = {
    "indent", YES
  };

  /** Specific serialization parameter: number of spaces to indent. */
  public static final Object[] S_INDENT_SPACES = {
    "indent-spaces", 2
  };

  /** Serialize REST: yes/no. */
  public static final Object[] S_REST = {
    "serialize-rest", NO
  };

  /** Serialize XML declaration: yes/no. */
  public static final Object[] S_XMLDECL = {
    "xmldecl", YES
  };

  /** Property file. */
  private static String mFilePath;

  /** Properties. */
  private final ConcurrentMap<String, Object> mProps = new ConcurrentHashMap<String, Object>();

  /**
   * Constructor.
   */
  public XMLSerializerProperties() {
    try {
      for (final Field f : getClass().getFields()) {
        final Object obj = f.get(null);
        if (!(obj instanceof Object[])) {
          continue;
        }
        final Object[] arr = (Object[])obj;
        mProps.put(arr[0].toString(), arr[1]);
      }
    } catch (final IllegalArgumentException | IllegalAccessException e) {
      e.printStackTrace();
    }
  }

  /**
   * <h2>Read properties</h2>
   * 
   * <p>
   * Read properties file into a concurrent HashMap. Format of properties file:
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
   * Note that currently all properties have to be set. If specific key/value pairs are specified more than
   * once the last values are preserved, so the default values are overridden by user specified values.
   * </p>
   * 
   * @param paramFilePath
   *          Path to properties file.
   * @return ConcurrentMap which holds property key/values.
   */
  public ConcurrentMap<String, Object> readProps(final String paramFilePath) {
    mFilePath = paramFilePath;
    if (!new File(mFilePath).exists()) {
      throw new IllegalStateException("Properties file doesn't exist!");
    }

    try {
      // Read and parse file.
      final BufferedReader buffReader = new BufferedReader(new FileReader(mFilePath));
      for (String line = buffReader.readLine(); line != null; line = buffReader.readLine()) {
        line = line.trim();
        if (line.isEmpty()) {
          continue;
        }

        final int equals = line.indexOf('=');
        if (equals < 0) {
          System.out.println("Properties file has no '=' sign in line -- parsing error!");
        }

        final String key = line.substring(0, equals).toUpperCase();
        final Object value = line.substring(equals + 1);

        mProps.put(key, value);
        buffReader.close();
      }
    } catch (final IOException exc) {
      exc.printStackTrace();
    }

    return mProps;
  }

  // /**
  // * Writes the properties to disk.
  // */
  // public final synchronized void write() {
  // final File file = new File(mFilePath);
  //
  // try {
  // // User has already specified key/values, so cache it.
  // final StringBuilder strBuilder = new StringBuilder();
  // if (file.exists()) {
  // final BufferedReader buffReader = new BufferedReader(new FileReader(file));
  //
  // for (String line = buffReader.readLine(); line != null; line = buffReader.readLine()) {
  // strBuilder.append(line + ECharsForSerializing.NEWLINE);
  // }
  //
  // buffReader.close();
  // }
  //
  // // Write map properties to file.
  // final BufferedWriter buffWriter = new BufferedWriter(new FileWriter(file));
  // for (final Field f : getClass().getFields()) {
  // final Object obj = f.get(null);
  // if (!(obj instanceof Object[]))
  // continue;
  // final String key = ((Object[])obj)[0].toString();
  // final Object value = ((Object[])obj)[1];
  // buffWriter.write(key + " = " + value + ECharsForSerializing.NEWLINE);
  // }
  //
  // // Append cached properties.
  // buffWriter.write(strBuilder.toString());
  // buffWriter.close();
  // } catch (final Exception e) {
  // LOGGER.error(e.getMessage(), e);
  // }
  // }

  /**
   * Get properties map.
   * 
   * @return ConcurrentMap with key/value property pairs.
   */
  public ConcurrentMap<String, Object> getProps() {
    return mProps;
  }

}
