/*
 * [New BSD License] Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org> All rights
 * reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the Brackit Project Team nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.xquery.function;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.atomic.Bool;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.jdm.Sequence;
import org.sirix.xquery.function.sdb.SDBFun;

import java.util.Arrays;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public final class FunUtil {

  public static int getInt(Sequence[] params, int pos, String parameterName, int defaultValue,
      int[] allowedValues, boolean required) throws QueryException {
    if (pos >= params.length || params[pos] == null) {
      if (required) {
        throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT,
            "Invalid integer parameter %s. Expected %s", parameterName,
            Arrays.toString(allowedValues));
      }

      return defaultValue;
    }

    final int value = ((IntNumeric) params[pos]).intValue();

    if (allowedValues == null) {
      return value;
    }

    for (final int allowedValue : allowedValues) {
      if (value == allowedValue) {
        return value;
      }
    }

    throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT,
        "Invalid integer parameter %s. Expected %s", parameterName, Arrays.toString(allowedValues));
  }

  public static long getLong(Sequence[] params, int pos, String parameterName, long defaultValue,
      long[] allowedValues, boolean required) throws QueryException {
    if (pos >= params.length || params[pos] == null) {
      if (required) {
        throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT,
            "Invalid long parameter %s. Expected %s", parameterName,
            Arrays.toString(allowedValues));
      }

      return defaultValue;
    }

    final long value = ((IntNumeric) params[pos]).longValue();

    if (allowedValues == null) {
      return value;
    }

    for (final long allowedValue : allowedValues) {
      if (value == allowedValue) {
        return value;
      }
    }

    throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT, "Invalid long parameter %s. Expected %s",
        parameterName, Arrays.toString(allowedValues));
  }

  public static boolean getBoolean(Sequence[] params, int pos, String parameterName,
      boolean defaultValue, boolean required) throws QueryException {
    if (pos >= params.length || params[pos] == null) {
      if (required) {
        throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT, "Invalid empty boolean parameter %s.",
            parameterName);
      }

      return defaultValue;
    }

    return ((Bool) params[pos]).bool;
  }

  public static String getString(Sequence[] params, int pos, String parameterName,
      String defaultValue, String[] allowedValues, boolean required) throws QueryException {
    if (pos >= params.length || params[pos] == null) {
      if (required) {
        throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT, "Invalid parameter %s. Expected %s",
            parameterName, Arrays.toString(allowedValues));
      }

      return defaultValue;
    }

    String value = ((Atomic) params[pos]).stringValue();

    if (allowedValues == null) {
      return value;
    }

    for (String allowedValue : allowedValues) {
      if (value.equals(allowedValue)) {
        return value;
      }
    }

    throw new QueryException(SDBFun.ERR_INVALID_ARGUMENT,
        "Invalid string parameter %s. Expected %s", parameterName, Arrays.toString(allowedValues));
  }
}
