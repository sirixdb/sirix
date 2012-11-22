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

package org.sirix.access;

import static org.junit.Assert.assertEquals;

import javax.xml.namespace.QName;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.PostOrderAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.DocumentCreater;

public class MultipleCommitTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		holder = Holder.generateWtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void test() throws SirixException {
		Assert.assertEquals(0L, holder.getWtx().getRevisionNumber());

		holder.getWtx().commit();

		holder.getWtx().insertElementAsFirstChild(new QName("foo"));
		assertEquals(1L, holder.getWtx().getRevisionNumber());
		holder.getWtx().moveTo(1);
		assertEquals(new QName("foo"), holder.getWtx().getName());
		holder.getWtx().abort();

		assertEquals(1L, holder.getWtx().getRevisionNumber());
	}

	@Test
	public void testAutoCommit() throws SirixException {
		DocumentCreater.create(holder.getWtx());
		holder.getWtx().commit();

		final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
		rtx.close();
	}

	@Test
	public void testRemove() throws SirixException {
		DocumentCreater.create(holder.getWtx());
		holder.getWtx().commit();
		assertEquals(1L, holder.getWtx().getRevisionNumber());

		holder.getWtx().moveToDocumentRoot();
		holder.getWtx().moveToFirstChild();
		holder.getWtx().remove();
		holder.getWtx().commit();
		assertEquals(2L, holder.getWtx().getRevisionNumber());
	}

	@Test
	public void testAttributeRemove() throws SirixException {
		DocumentCreater.create(holder.getWtx());
		holder.getWtx().commit();
		holder.getWtx().moveToDocumentRoot();

		final AbstractAxis postorderAxis = new PostOrderAxis(holder.getWtx());
		while (postorderAxis.hasNext()) {
			postorderAxis.next();
			if (holder.getWtx().getKind() == Kind.ELEMENT
					&& holder.getWtx().getAttributeCount() > 0) {
				for (int i = 0, attrCount = holder.getWtx().getAttributeCount(); i < attrCount; i++) {
					holder.getWtx().moveToAttribute(i);
					holder.getWtx().remove();
				}
			}
		}
		holder.getWtx().commit();
		holder.getWtx().moveToDocumentRoot();

		int attrTouch = 0;
		final Axis descAxis = new DescendantAxis(holder.getWtx());
		while (descAxis.hasNext()) {
			descAxis.next();
			if (holder.getWtx().getKind() == Kind.ELEMENT) {
				for (int i = 0, attrCount = holder.getWtx().getAttributeCount(); i < attrCount; i++) {
					if (holder.getWtx().moveToAttribute(i).hasMoved()) {
						attrTouch++;
					} else {
						throw new IllegalStateException("Should never occur!");
					}
				}
			}
		}
		assertEquals(0, attrTouch);

	}
}
