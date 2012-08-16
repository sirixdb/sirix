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

import java.util.Random;

import javax.xml.namespace.QName;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.exception.AbsTTException;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.settings.EFixed;

public final class OverallTest extends TestCase {

  private static int NUM_CHARS = 3;
  private static int ELEMENTS = 1000;
  private static int COMMITPERCENTAGE = 20;
  private static int REMOVEPERCENTAGE = 20;
  private static final Random ran = new Random(0l);
  public static String chars = "abcdefghijklm";

  private Holder holder;

  @Override
  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @Test
  public void testJustEverything() throws AbsTTException {
    holder.getWtx().insertElementAsFirstChild(new QName(getString()));
    holder.getWtx().insertElementAsFirstChild(new QName(getString()));
    for (int i = 0; i < ELEMENTS; i++) {
      if (ran.nextBoolean()) {
        switch (holder.getWtx().getNode().getKind()) {
        case ELEMENT:
//          holder.getWtx().setQName(new QName(getString()));
          break;
        case ATTRIBUTE:
//          holder.getWtx().setQName(new QName(getString()));
          holder.getWtx().setValue(getString());
          break;
        case NAMESPACE:
//          holder.getWtx().setQName(new QName(getString()));
          break;
        case TEXT:
          holder.getWtx().setValue(getString());
          break;
        default:
        }
      } else {
        if (holder.getWtx().getNode() instanceof ElementNode) {
          if (holder.getWtx().getNode().getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
            assertTrue(holder.getWtx().moveToFirstChild());
            assertTrue(holder.getWtx().moveToFirstChild());
          }
          if (ran.nextBoolean()) {
            holder.getWtx().insertElementAsFirstChild(new QName(getString()));
          } else {
            holder.getWtx().insertElementAsRightSibling(new QName(getString()));
          }
          if (ran.nextBoolean()) {
            holder.getWtx().insertAttribute(new QName(getString()), getString());
            holder.getWtx().moveToParent();
          }
          if (ran.nextBoolean()) {
            holder.getWtx().insertNamespace(new QName(getString(), getString()));
            holder.getWtx().moveToParent();
          }
        }

        if (ran.nextInt(100) < REMOVEPERCENTAGE) {
          holder.getWtx().remove();
        }

        if (ran.nextInt(100) < COMMITPERCENTAGE) {
          holder.getWtx().commit();
        }
        do {
          final int newKey = ran.nextInt(i + 1) + 1;

          if (newKey == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
            holder.getWtx().moveToFirstChild();
            holder.getWtx().moveToFirstChild();
          } else {
            holder.getWtx().moveTo(newKey);
            if (holder.getWtx().getNode().getParentKey() == EFixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
              holder.getWtx().moveToFirstChild();
            }
          }
        } while (holder.getWtx().getNode() == null);
        // TODO Check if reference check can occur on "=="
        if (holder.getWtx().getNode().getKind() != EKind.ELEMENT) {
          holder.getWtx().moveToParent();
        }
      }
    }
    final long key = holder.getWtx().getNode().getNodeKey();
    holder.getWtx().remove();
    holder.getWtx().insertElementAsFirstChild(new QName(getString()));
    holder.getWtx().moveTo(key);
    holder.getWtx().commit();
    holder.getWtx().close();
  }

  @Override
  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  private static String getString() {
    char[] buf = new char[NUM_CHARS];

    for (int i = 0; i < buf.length; i++) {
      buf[i] = chars.charAt(ran.nextInt(chars.length()));
    }

    return new String(buf);
  }

}
