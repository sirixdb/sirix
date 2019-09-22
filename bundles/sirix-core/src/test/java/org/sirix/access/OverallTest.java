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

import java.util.Random;
import junit.framework.TestCase;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;
import org.sirix.settings.Fixed;

/** Test a bunch of modification methods. */
public final class OverallTest extends TestCase {

  /** Used for random number generator. */
  private static int NUM_CHARS = 3;

  /** Modification number of nodes. */
  private static int ELEMENTS = 1000;

  /** Percentage of commits. */
  private static int COMMITPERCENTAGE = 20;

  /** Percentage of nodes to remove. */
  private static int REMOVEPERCENTAGE = 20;

  /** Random number generator. */
  private static final Random ran = new Random(0l);

  /** Some characters. */
  public static String chars = "abcdefghijklm";

  /** {@link Holder} instance. */
  private Holder holder;

  @Override
  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateWtx();
  }

  @Test
  public void testJustEverything() throws SirixException {
    holder.getXdmNodeWriteTrx().insertElementAsFirstChild(new QNm(getString()));
    holder.getXdmNodeWriteTrx().insertElementAsFirstChild(new QNm(getString()));
    for (int i = 0; i < ELEMENTS; i++) {
      if (ran.nextBoolean()) {
        switch (holder.getXdmNodeWriteTrx().getKind()) {
          case ELEMENT:
            holder.getXdmNodeWriteTrx().setName(new QNm(getString()));
            break;
          case ATTRIBUTE:
            holder.getXdmNodeWriteTrx().setName(new QNm(getString()));
            holder.getXdmNodeWriteTrx().setValue(getString());
            break;
          case NAMESPACE:
            holder.getXdmNodeWriteTrx().setName(new QNm(getString()));
            break;
          case PROCESSING_INSTRUCTION:
          case TEXT:
          case COMMENT:
            holder.getXdmNodeWriteTrx().setValue(getString());
            break;
          // $CASES-OMITTED$
          default:
        }
      } else {
        if (holder.getXdmNodeWriteTrx().getKind() == NodeKind.ELEMENT) {
          if (holder.getXdmNodeWriteTrx()
                    .getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
            assertTrue(holder.getXdmNodeWriteTrx().moveToFirstChild().hasMoved());
            assertTrue(holder.getXdmNodeWriteTrx().moveToFirstChild().hasMoved());
          }
          if (ran.nextBoolean()) {
            holder.getXdmNodeWriteTrx().insertElementAsFirstChild(new QNm(getString()));
          } else {
            holder.getXdmNodeWriteTrx().insertElementAsRightSibling(new QNm(getString()));
          }
          if (ran.nextBoolean()) {
            holder.getXdmNodeWriteTrx().insertAttribute(new QNm(getString()), getString());
            holder.getXdmNodeWriteTrx().moveToParent();
          }
          if (ran.nextBoolean()) {
            holder.getXdmNodeWriteTrx().insertNamespace(new QNm(getString(), getString()));
            holder.getXdmNodeWriteTrx().moveToParent();
          }
        }

        if (ran.nextInt(100) < REMOVEPERCENTAGE) {
          holder.getXdmNodeWriteTrx().remove();
        }

        if (ran.nextInt(100) < COMMITPERCENTAGE) {
          holder.getXdmNodeWriteTrx().commit();
        }
        do {
          final int newKey = ran.nextInt(i + 1) + 1;

          if (newKey == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
            holder.getXdmNodeWriteTrx().moveToFirstChild();
            holder.getXdmNodeWriteTrx().moveToFirstChild();
          } else {
            holder.getXdmNodeWriteTrx().moveTo(newKey);
            if (holder.getXdmNodeWriteTrx()
                      .getParentKey() == Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
              holder.getXdmNodeWriteTrx().moveToFirstChild();
            }
          }
        } while (holder.getXdmNodeWriteTrx() == null);
        if (holder.getXdmNodeWriteTrx().getKind() != NodeKind.ELEMENT) {
          holder.getXdmNodeWriteTrx().moveToParent();
        }
      }
    }
    final long key = holder.getXdmNodeWriteTrx().getNodeKey();
    holder.getXdmNodeWriteTrx().remove();
    holder.getXdmNodeWriteTrx().insertElementAsFirstChild(new QNm(getString()));
    holder.getXdmNodeWriteTrx().moveTo(key);
    holder.getXdmNodeWriteTrx().commit();
    holder.getXdmNodeWriteTrx().close();
  }

  @Override
  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  /** Get a random string. */
  private static String getString() {
    char[] buf = new char[NUM_CHARS];

    for (int i = 0; i < buf.length; i++) {
      buf[i] = chars.charAt(ran.nextInt(chars.length()));
    }

    return new String(buf);
  }

}
