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

package org.sirix.axis;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.utils.DocumentCreater;

/**
 * Test {@link PostOrderAxis}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PostOrderTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		TestHelper.createTestDocument();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testIterateWhole() throws SirixException {
		final NodeReadTrx rtx = holder.getRtx();

		rtx.moveToDocumentRoot();
		AbsAxisTest.testIAxisConventions(new PostOrderAxis(rtx), new long[] { 4L,
				6L, 7L, 5L, 8L, 11L, 12L, 9L, 13L, 1L, 0L });
	}

	@Test
	public void testIterateFirstSubtree() throws SirixException {
		final NodeReadTrx rtx = holder.getRtx();

		rtx.moveTo(5);
		AbsAxisTest.testIAxisConventions(new PostOrderAxis(rtx), new long[] { 6L,
				7L });
	}

	@Test
	public void testIterateZero() throws SirixException {
		final NodeReadTrx rtx = holder.getRtx();

		rtx.moveTo(8);
		AbsAxisTest.testIAxisConventions(new PostOrderAxis(rtx), new long[] {});
	}

	@Test
	public void testIterateDocumentFirst() throws SirixException, IOException,
			XMLStreamException {
		try (final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx()) {
			wtx.moveTo(9);
			wtx.insertSubtree(
					XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
					Insert.ASFIRSTCHILD);
			wtx.commit();
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx), new long[] { 17,
					19, 20, 18, 21, 24, 25, 22, 26 });
			wtx.moveTo(14);
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx, IncludeSelf.YES),
					new long[] { 17, 19, 20, 18, 21, 24, 25, 22, 26, 14 });
			wtx.moveToDocumentRoot();
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx), new long[] { 4L,
					6L, 7L, 5L, 8L, 17, 19, 20, 18, 21, 24, 25, 22, 26, 14, 11L, 12L, 9L,
					13L, 1L });
			wtx.moveToDocumentRoot();
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx, IncludeSelf.YES),
					new long[] { 4L, 6L, 7L, 5L, 8L, 17, 19, 20, 18, 21, 24, 25, 22, 26,
							14, 11L, 12L, 9L, 13L, 1L, 0L });
		}
	}

	@Test
	public void testIterateDocumentSecond() throws SirixException, IOException,
			XMLStreamException {
		try (final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx()) {
			wtx.moveTo(11);
			wtx.insertSubtree(
					XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
					Insert.ASFIRSTCHILD);
			wtx.commit();
			wtx.moveToDocumentRoot();
			wtx.moveToFirstChild();
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx, IncludeSelf.YES),
					new long[] { 4L, 6L, 7L, 5L, 8L, 17, 19, 20, 18, 21, 24, 25, 22, 26,
							14, 11L, 12L, 9L, 13L, 1L });
			wtx.moveToDocumentRoot();
			wtx.moveToFirstChild();
			AbsAxisTest.testIAxisConventions(new PostOrderAxis(wtx), new long[] { 4L,
					6L, 7L, 5L, 8L, 17, 19, 20, 18, 21, 24, 25, 22, 26, 14, 11L, 12L, 9L,
					13L });
		}
	}
}
