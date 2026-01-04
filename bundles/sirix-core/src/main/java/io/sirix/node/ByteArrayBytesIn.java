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

package io.sirix.node;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * BytesIn implementation backed by a byte array.
 * Used for lazy snapshot parsing where the node owns a copy of the serialized bytes.
 *
 * @author Johannes Lichtenberger
 */
public final class ByteArrayBytesIn implements BytesIn<byte[]> {

  private final byte[] data;
  private int position;

  /**
   * Create a new ByteArrayBytesIn from a byte array.
   *
   * @param data the byte array to read from
   */
  public ByteArrayBytesIn(byte[] data) {
    this.data = data;
    this.position = 0;
  }

  @Override
  public byte[] getSource() {
    return data;
  }

  @Override
  public byte[] getUnderlying() {
    return data;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public void position(long newPosition) {
    this.position = (int) newPosition;
  }

  @Override
  public void skip(long bytes) {
    this.position += (int) bytes;
  }

  @Override
  public long remaining() {
    return data.length - position;
  }

  @Override
  public byte readByte() {
    return data[position++];
  }

  @Override
  public short readShort() {
    short value = (short) ((data[position] & 0xFF) | ((data[position + 1] & 0xFF) << 8));
    position += 2;
    return value;
  }

  @Override
  public int readInt() {
    int value = (data[position] & 0xFF) |
                ((data[position + 1] & 0xFF) << 8) |
                ((data[position + 2] & 0xFF) << 16) |
                ((data[position + 3] & 0xFF) << 24);
    position += 4;
    return value;
  }

  @Override
  public long readLong() {
    long value = (data[position] & 0xFFL) |
                 ((data[position + 1] & 0xFFL) << 8) |
                 ((data[position + 2] & 0xFFL) << 16) |
                 ((data[position + 3] & 0xFFL) << 24) |
                 ((data[position + 4] & 0xFFL) << 32) |
                 ((data[position + 5] & 0xFFL) << 40) |
                 ((data[position + 6] & 0xFFL) << 48) |
                 ((data[position + 7] & 0xFFL) << 56);
    position += 8;
    return value;
  }

  @Override
  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public boolean readBoolean() {
    return readByte() != 0;
  }

  @Override
  public void read(byte[] dest) {
    System.arraycopy(data, position, dest, 0, dest.length);
    position += dest.length;
  }

  @Override
  public void read(byte[] dest, int offset, int length) {
    System.arraycopy(data, position, dest, offset, length);
    position += length;
  }

  @Override
  public long readStopBit() {
    long result = 0;
    int shift = 0;
    while (true) {
      byte b = readByte();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
    }
    return result;
  }

  @Override
  public String readUtf8() {
    int length = (int) readStopBit();
    if (length < 0) {
      return null;
    }
    byte[] bytes = new byte[length];
    read(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public BigInteger readBigInteger() {
    int length = readInt();
    byte[] bytes = new byte[length];
    read(bytes);
    return new BigInteger(bytes);
  }

  @Override
  public InputStream inputStream() {
    return new ByteArrayInputStream(data, position, data.length - position);
  }

  @Override
  public byte[] toByteArray() {
    return Arrays.copyOfRange(data, position, data.length);
  }
}








