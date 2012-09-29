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
package org.sirix.diff;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.stream.XMLStreamException;

import org.mockito.InOrder;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.diff.DiffFactory.EDiff;
import org.sirix.diff.DiffFactory.EDiffOptimized;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.service.xml.shredder.EShredderCommit;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.shredder.XMLUpdateShredder;
import org.sirix.utils.DocumentCreater;

public final class DiffTestHelper {

  protected static final String RESOURCES = "src" + File.separator + "test" + File.separator + "resources";
  protected static final long TIMEOUT_S = 5;

  static void setUp() throws SirixException {
    TestHelper.deleteEverything();
  }

  static void setUpFirst(final Holder pHolder) throws SirixException {
    DocumentCreater.createVersioned(pHolder.getWtx());
  }

  static void setUpSecond(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    initializeData(pHolder, new File(RESOURCES + File.separator + "revXMLsAll4" + File.separator
      + "1.xml"), new File(RESOURCES + File.separator + "revXMLsAll4" + File.separator + "2.xml"));
  }

  static void setUpThird(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    new XMLShredder(pHolder.getWtx(), XMLShredder.createFileReader(new File(RESOURCES + File.separator
      + "revXMLsDelete1" + File.separator + "1.xml")), EInsert.ASFIRSTCHILD).call();
    final INodeWriteTrx wtx = pHolder.getWtx();
    wtx.moveToDocumentRoot();
    wtx.moveToFirstChild();
    wtx.moveToFirstChild();
    wtx.remove();
    wtx.moveToRightSibling();
    wtx.remove();
    wtx.moveToFirstChild();
    wtx.remove();
    wtx.moveToRightSibling();
    wtx.remove();
    wtx.commit();
  }

  static void setUpFourth(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    initializeData(pHolder, new File(RESOURCES + File.separator + "revXMLsAll3" + File.separator
      + "1.xml"), new File(RESOURCES + File.separator + "revXMLsAll3" + File.separator + "2.xml"));
  }

  static void setUpFifth(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    initializeData(pHolder, new File(RESOURCES + File.separator + "revXMLsAll2" + File.separator
      + "1.xml"), new File(RESOURCES + File.separator + "revXMLsAll2" + File.separator + "2.xml"));
  }

  static void setUpSixth(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    initializeData(pHolder, new File(RESOURCES + File.separator + "revXMLsDelete2" + File.separator
      + "1.xml"), new File(RESOURCES + File.separator + "revXMLsDelete2" + File.separator + "2.xml"));
  }

  static void setUpSeventh(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = pHolder.getWtx();
    DocumentCreater.create(wtx);
    wtx.commit();
    final INodeReadTrx rtx = pHolder.getSession().beginNodeReadTrx(0);
    rtx.moveTo(1);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    wtx.commit();
    rtx.close();
  }

  static void setUpEighth(final Holder pHolder) throws SirixException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = pHolder.getWtx();
    DocumentCreater.create(wtx);
    wtx.commit();
    final INodeReadTrx rtx = pHolder.getSession().beginNodeReadTrx(0);
    rtx.moveTo(11);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    wtx.commit();
    rtx.close();
  }

  private static void initializeData(final Holder pHolder, final File... pFile)
    throws SirixException, IOException, XMLStreamException {

    int i = 0;
    for (final File file : pFile) {
      XMLShredder init;
      if (i == 0) {
        init =
          new XMLShredder(pHolder.getWtx(), XMLShredder.createFileReader(file),
            EInsert.ASFIRSTCHILD);
      } else {
        init =
          new XMLUpdateShredder(pHolder.getWtx(), XMLShredder.createFileReader(file),
            EInsert.ASFIRSTCHILD, file, EShredderCommit.COMMIT);
      }
      i++;
      init.call();
    }

  }

  static IDiffObserver createMock() {
    return mock(IDiffObserver.class);
  }

  static void verifyDiffFirst(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.INSERTED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(9)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyOptimizedFirst(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.INSERTED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(5)).diffListener(eq(EDiff.SAMEHASH), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffSecond(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.REPLACEDNEW), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(4)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyOptimizedSecond(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.REPLACEDNEW), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.SAMEHASH), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffThird(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyOptimizedThird(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAMEHASH), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAMEHASH), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffFourth(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.INSERTED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyOptimizedFourth(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.INSERTED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAMEHASH), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffFifth(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.UPDATED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffSixth(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(eq(EDiff.DELETED), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffSeventh(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(10)).diffListener(eq(EDiff.REPLACEDNEW), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(5)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffEighth(final IDiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(2)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(3)).diffListener(eq(EDiff.REPLACEDOLD), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(5)).diffListener(eq(EDiff.SAME), isA(Long.class),
      isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void checkFullDiff(final Holder pHolder, final IDiffObserver pObserver,
    final EDiffOptimized pOptimized) throws SirixException, InterruptedException {
    final Set<IDiffObserver> observers = new HashSet<IDiffObserver>();
    observers.add(pObserver);
    DiffFactory.invokeFullDiff(new DiffFactory.Builder(pHolder.getSession(), 1, 0, pOptimized,
      observers));
  }

  static void checkStructuralDiff(final Holder pHolder, final IDiffObserver pObserver,
    final EDiffOptimized pOptimized) throws SirixException, InterruptedException {
    final Set<IDiffObserver> observers = new HashSet<IDiffObserver>();
    observers.add(pObserver);
    DiffFactory.invokeStructuralDiff(new DiffFactory.Builder(pHolder.getSession(), 1, 0, pOptimized,
      observers));
  }
}
