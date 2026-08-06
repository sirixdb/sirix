/*
 * Copyright (c) 2026, Sirix Contributors
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
package io.sirix.benchmark;

import java.io.Reader;
import java.util.Random;

/**
 * Generates a JSON array {@code [{"id":i,"age":..,"dept":..,"city":..,"active":..}, ...]}
 * lazily into a char buffer so callers (Gson's JsonReader) can shred arbitrarily large
 * datasets without materializing the JSON string. Fixed seed 42 — every bench built on
 * this reader sees the identical dataset for a given record count.
 */
final class GeneratedRecordsReader extends Reader {

  static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };

  private final long total;
  private final Random rng = new Random(42);
  private final StringBuilder line = new StringBuilder(96);
  private long produced = 0;
  private int pos = 0;
  private boolean opened = false;
  private boolean closed = false;

  GeneratedRecordsReader(final long total) {
    this.total = total;
  }

  private void refill() {
    line.setLength(0);
    pos = 0;
    if (!opened) {
      line.append('[');
      opened = true;
      return;
    }
    if (produced < total) {
      if (produced > 0) {
        line.append(',');
      }
      line.append("{\"id\":").append(produced)
          .append(",\"age\":").append(18 + rng.nextInt(48))
          .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
          .append("\",\"city\":\"").append(CITIES[rng.nextInt(CITIES.length)])
          .append("\",\"active\":").append(rng.nextBoolean() ? "true" : "false")
          .append('}');
      produced++;
      return;
    }
    if (!closed) {
      line.append(']');
      closed = true;
    }
  }

  @Override
  public int read(final char[] cbuf, final int off, final int len) {
    if (pos >= line.length()) {
      if (closed) {
        return -1;
      }
      refill();
      if (pos >= line.length()) {
        return -1;
      }
    }
    final int n = Math.min(len, line.length() - pos);
    line.getChars(pos, pos + n, cbuf, off);
    pos += n;
    return n;
  }

  @Override
  public void close() {
    // no-op
  }
}
