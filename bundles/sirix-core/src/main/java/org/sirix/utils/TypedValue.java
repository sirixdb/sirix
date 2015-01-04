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

package org.sirix.utils;

import org.sirix.settings.Constants;

/**
 * <h1>UTF</h1>
 * 
 * <p>
 * Util to efficiently convert byte arrays to various Java types and vice versa.
 * It also provides efficient comparison and hash methods.
 * </p>
 */
public final class TypedValue {

	/** Empty string. */
	public static final byte[] EMPTY = new byte[0];

	/**
	 * Hidden constructor.
	 */
	private TypedValue() {
		// Hidden.
	}

	/**
	 * Parse int from given UTF-8 byte array.
	 * 
	 * @param mBytes
	 *          Byte array to parse int from.
	 * @return Int.
	 */
	public static int parseInt(final byte[] mBytes) {
		int position = 0;
		int value = ((mBytes[position++] & 127));
		if ((mBytes[position - 1] & 128) != 0) {
			value |= ((mBytes[position++] & 127)) << 7;
			if ((mBytes[position - 1] & 128) != 0) {
				value |= ((mBytes[position++] & 127)) << 14;
				if ((mBytes[position - 1] & 128) != 0) {
					value |= ((mBytes[position++] & 127)) << 21;
					if ((mBytes[position - 1] & 128) != 0) {
						value |= ((mBytes[position++] & 255)) << 28;
					} else if ((mBytes[position - 1] & 64) != 0)
						value |= 0xF0000000;
				} else if ((mBytes[position - 1] & 64) != 0)
					value |= 0xFFF00000;
			} else if ((mBytes[position - 1] & 64) != 0)
				value |= 0xFFFFE000;
		} else if ((mBytes[position - 1] & 64) != 0)
			value |= 0xFFFFFFC0;
		return value;
	}

	/**
	 * Parse long from given UTF-8 byte array.
	 * 
	 * @param mBytes
	 *          Byte array to parse long from.
	 * @return Long.
	 */
	public static long parseLong(final byte[] mBytes) {
		int position = 1;
		long value = (mBytes[position++] & 255);
		if (mBytes[position - 2] > 1) {
			value += ((long) (mBytes[position++] & 255) << 8);
			if (mBytes[position - 3] > 2) {
				value += ((long) (mBytes[position++] & 255) << 16);
				if (mBytes[position - 4] > 3) {
					value += ((long) (mBytes[position++] & 255) << 24);
					if (mBytes[position - 5] > 4) {
						value += ((long) (mBytes[position++] & 255) << 32);
						if (mBytes[position - 6] > 5) {
							value += ((long) (mBytes[position++] & 255) << 40);
							if (mBytes[position - 7] > 6) {
								value += ((long) (mBytes[position++] & 255) << 48);
								if (mBytes[position - 8] > 7) {
									value += ((long) mBytes[position++] << 56);
								} else if ((mBytes[position - 1] & 128) != 0)
									value |= 0xFF000000000000L;
							} else if ((mBytes[position - 1] & 128) != 0)
								value |= 0xFFFF000000000000L;
						} else if ((mBytes[position - 1] & 128) != 0)
							value |= 0xFFFFFF0000000000L;
					} else if ((mBytes[position - 1] & 128) != 0)
						value |= 0xFFFFFFFF00000000L;
				} else if ((mBytes[position - 1] & 128) != 0)
					value |= 0xFFFFFFFFFF000000L;
			} else if ((mBytes[position - 1] & 128) != 0)
				value |= 0xFFFFFFFFFFFF0000L;
		} else if ((mBytes[position - 1] & 128) != 0)
			value |= 0xFFFFFFFFFFFFFF00L;
		return value;
	}

	/**
	 * Get UTF-8 byte array from int. The given byte array yields a string
	 * representation if read with parseString().
	 * 
	 * @param mValue
	 *          Int to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of int.
	 */
	public static byte[] getBytes(final boolean mValue) {
		final byte[] bytes = new byte[1];
		if (mValue) {
			bytes[0] = 1;
		} else {
			bytes[0] = 0;
		}
		return bytes;
	}

	/**
	 * Get UTF-8 byte array from int. The given byte array yields a string
	 * representation if read with parseString().
	 * 
	 * @param mValue
	 *          Int to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of int.
	 */
	public static byte[] getBytes(final int mValue) {
		final byte[] tmpBytes = new byte[5];
		int position = 0;
		tmpBytes[position++] = (byte) (mValue);
		if (mValue > 63 || mValue < -64) {
			tmpBytes[position - 1] |= 128;
			tmpBytes[position++] = (byte) (mValue >> 7);
			if (mValue > 8191 || mValue < -8192) {
				tmpBytes[position - 1] |= 128;
				tmpBytes[position++] = (byte) (mValue >> 14);
				if (mValue > 1048575 || mValue < -1048576) {
					tmpBytes[position - 1] |= 128;
					tmpBytes[position++] = (byte) (mValue >> 21);
					if (mValue > 268435455 || mValue < -268435456) {
						tmpBytes[position - 1] |= 128;
						tmpBytes[position++] = (byte) (mValue >> 28);
					} else {
						tmpBytes[position - 1] &= 127;
					}
				} else {
					tmpBytes[position - 1] &= 127;
				}
			} else {
				tmpBytes[position - 1] &= 127;
			}
		} else {
			tmpBytes[position - 1] &= 127;
		}

		final byte[] bytes = new byte[position];
		System.arraycopy(tmpBytes, 0, bytes, 0, position);
		return bytes;
	}

	/**
	 * Get UTF-8 byte array from long. The given byte array yields a string
	 * representation if read with parseString().
	 * 
	 * @param value
	 *          Long to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of long.
	 */
	public static byte[] getBytes(final long value) {
		final byte[] tmpBytes = new byte[9];
		int position = 1;
		tmpBytes[position++] = (byte) value;
		if (value > 127 || value < -128) {
			tmpBytes[position++] = (byte) (value >> 8);
			if (value > 32767 || value < -32768) {
				tmpBytes[position++] = (byte) (value >>> 16);
				if (value > 8388607 || value < -8388608) {
					tmpBytes[position++] = (byte) (value >>> 24);
					if (value > 2147483647 || value < -2147483648) {
						tmpBytes[position++] = (byte) (value >>> 32);
						if (value > (2 ^ 39) - 1 || value < -(2 ^ 39)) {
							tmpBytes[position++] = (byte) (value >>> 40);
							if (value > (2 ^ 47) - 1 || value < -(2 ^ 47)) {
								tmpBytes[position++] = (byte) (value >>> 48);
								if (value > (2 ^ 55) - 1 || value < -(2 ^ 55)) {
									tmpBytes[position++] = (byte) (value >>> 56);
									tmpBytes[position - 9] = (byte) 8;
								} else {
									tmpBytes[position - 8] = (byte) 7;
								}
							} else {
								tmpBytes[position - 7] = (byte) 6;
							}
						} else {
							tmpBytes[position - 6] = (byte) 5;
						}
					} else {
						tmpBytes[position - 5] = (byte) 4;
					}
				} else {
					tmpBytes[position - 4] = (byte) 3;
				}
			} else {
				tmpBytes[position - 3] = (byte) 2;
			}
		} else {
			tmpBytes[position - 2] = (byte) 1;
		}

		final byte[] bytes = new byte[position];
		System.arraycopy(tmpBytes, 0, bytes, 0, position);
		return bytes;
	}

	/**
	 * Get UTF-8 byte array from string. The given byte array yields a int if read
	 * with parseInt().
	 * 
	 * @param mValue
	 *          String to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of string.
	 */
	public static byte[] getBytes(final String mValue) {
		byte[] bytes = null;
		try {
			if (mValue == null || mValue.length() == 0) {
				bytes = EMPTY;
			} else {
				final StringBuilder builder = new StringBuilder();
				for (int i = 0; i < mValue.length(); i++) {
					switch (mValue.charAt(i)) {
					case '&':
						builder.append("&amp;");
						break;
					case '<':
						builder.append("&lt;");
						break;
					default:
						builder.append(mValue.charAt(i));
					}
				}
				bytes = builder.toString().getBytes(Constants.DEFAULT_ENCODING);

				// bytes = value.replace("&", "&amp;").replace("<", "&lt;")
				// .getBytes(IConstants.DEFAULT_ENCODING);
			}
		} catch (final Exception e) {
			throw new RuntimeException("Could not convert String to byte[]: "
					+ e.getLocalizedMessage());
		}
		return bytes;
	}

	public static boolean equals(final byte[] mValue1, final byte[] mValue2) {
		// Fail if one is null.
		if ((mValue1 == null) || (mValue2 == null)) {
			return false;
		}
		// Fail if the values are not of equal length.
		if (mValue1.length != mValue2.length) {
			return false;
		}
		// Fail if a single byte does not match.
		for (int i = 0, l = mValue1.length; i < l; i++) {
			if (mValue1[i] != mValue2[i]) {
				return false;
			}
		}
		// Values must be equal if we reach here.
		return true;
	}

	public static boolean equals(final byte[] mValue1, final String mValue2) {
		return equals(mValue1, TypedValue.getBytes(mValue2));
	}

	public static boolean equals(final String mValue1, final byte[] mValue2) {
		return equals(TypedValue.getBytes(mValue1), mValue2);
	}

	public static boolean equals(final String mValue1, final String mValue2) {
		return equals(TypedValue.getBytes(mValue1), TypedValue.getBytes(mValue2));
	}

	/**
	 * Get UTF-8 byte array from double. The given byte array yields a double if
	 * read with parseDouble().
	 * 
	 * @param mValue
	 *          double value to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of double.
	 */
	public static byte[] getBytes(final Double mValue) {
		return mValue.toString().getBytes();
	}

	/**
	 * Get UTF-8 byte array from float. The given byte array yields a float if
	 * read with parseFloat().
	 * 
	 * @param mValue
	 *          float to encode as UTF-8 byte array.
	 * @return UTF-8-encoded byte array of float.
	 */
	public static byte[] getBytes(final Float mValue) {
		return mValue.toString().getBytes();
	}

}
