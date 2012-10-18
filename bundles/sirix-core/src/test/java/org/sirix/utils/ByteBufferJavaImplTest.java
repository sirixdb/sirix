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

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class ByteBufferJavaImplTest {

	@Test(expected = IllegalArgumentException.class)
	public void testBasics() throws IllegalArgumentException {
		final ByteBuffer buffer = ByteBuffer.allocate(100);
		Assert.assertEquals(0, buffer.position());
		final IllegalArgumentException[] excsToFire = new IllegalArgumentException[2];
		try {
			buffer.position(101);
		} catch (final IllegalArgumentException exc) {
			excsToFire[0] = exc;
		}
		try {
			buffer.position(-100);
		} catch (final IllegalArgumentException exc) {
			excsToFire[1] = exc;
		}
		if (excsToFire[0] != null && excsToFire[1] != null) {
			throw excsToFire[0];
		}

	}

	@Test
	public void testPutGet() {
		final ByteBuffer buffer = ByteBuffer.allocate(100);
		// Byte.
		buffer.position(0);
		buffer.put((byte) 13);
		buffer.put(Byte.MAX_VALUE);
		Assert.assertEquals(2, buffer.position());
		buffer.position(0);
		Assert.assertEquals(13, buffer.get());
		Assert.assertEquals(Byte.MAX_VALUE, buffer.get());
		// Int.
		buffer.position(0);
		buffer.putInt(8192);
		buffer.putInt(Integer.MAX_VALUE);
		Assert.assertEquals(8, buffer.position());
		buffer.position(0);
		Assert.assertEquals(8192, buffer.getInt());
		Assert.assertEquals(Integer.MAX_VALUE, buffer.getInt());
		// Long.
		buffer.position(0);
		buffer.putLong(819281928192L);
		buffer.putLong(Long.MAX_VALUE);
		Assert.assertEquals(16, buffer.position());
		buffer.position(0);
		Assert.assertEquals(819281928192L, buffer.getLong());
		Assert.assertEquals(Long.MAX_VALUE, buffer.getLong());
	}

	@Test
	public void testPutGetArray() {
		final ByteBuffer buffer = ByteBuffer.allocate(100);

		final byte[] reference = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
		buffer.position(0);
		for (final byte byteVal : reference) {
			buffer.put(byteVal);
		}
		Assert.assertEquals(8, buffer.position());

		buffer.position(0);
		final byte[] result = new byte[8];
		for (int i = 0; i < result.length; i++) {
			result[i] = buffer.get();
		}
		Assert.assertEquals(8, buffer.position());
		Assert.assertArrayEquals(reference, result);
	}
}
