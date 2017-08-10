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

package org.sirix.access;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.Axis;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.api.XdmNodeWriteTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;

public class ThreadTest {

	public static final int WORKER_COUNT = 50;

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		TestHelper.createTestDocument();
		holder = Holder.openResourceManager();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testThreads() throws Exception {
		final ExecutorService taskExecutor = Executors.newFixedThreadPool(WORKER_COUNT);
		long newKey = 10L;
		for (int i = 0; i < WORKER_COUNT; i++) {
			taskExecutor.submit(new Task(holder.getResourceManager().beginNodeReadTrx(i)));

			try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
				wtx.moveTo(newKey);
				wtx.setValue("value" + i);
				newKey = wtx.getNodeKey();
				wtx.commit();
			}
		}
		taskExecutor.shutdown();
		taskExecutor.awaitTermination(1000000, TimeUnit.SECONDS);
	}

	private class Task implements Callable<Void> {

		private XdmNodeReadTrx mRTX;

		public Task(final XdmNodeReadTrx rtx) {
			mRTX = rtx;
		}

		@Override
		public Void call() throws Exception {
			final Axis axis = new DescendantAxis(mRTX);
			while (axis.hasNext()) {
				axis.next();
			}

			mRTX.moveTo(12L);
			assertEquals("bar", mRTX.getValue());
			mRTX.close();
			return null;
		}
	}

}
