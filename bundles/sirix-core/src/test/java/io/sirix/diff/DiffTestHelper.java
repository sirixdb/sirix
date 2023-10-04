/*
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
package io.sirix.diff;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.xml.stream.XMLStreamException;

import io.sirix.utils.XmlDocumentCreator;
import org.mockito.InOrder;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.diff.DiffFactory.DiffOptimized;
import io.sirix.diff.DiffFactory.DiffType;
import io.sirix.exception.SirixException;
import io.sirix.service.ShredderCommit;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.shredder.XMLUpdateShredder;
import io.sirix.service.xml.shredder.XmlShredder;
import com.google.common.collect.ImmutableSet;

public final class DiffTestHelper {

  private static final Path RESOURCES = Paths.get("src", "test", "resources");

  static void setUp() {
    XmlTestHelper.deleteEverything();
  }

  static void setUpFirst(final Holder holder) {
    XmlDocumentCreator.createVersioned(holder.getXdmNodeWriteTrx());
  }

  static void setUpSecond(final Holder holder)
      throws SirixException, IOException, XMLStreamException {
    initializeData(
        holder, RESOURCES.resolve("revXMLsAll4").resolve("1.xml"),
        RESOURCES.resolve("revXMLsAll4").resolve("2.xml"));
  }

  static void setUpThird(final Holder holder) throws IOException {
    try (final FileInputStream fis =
        new FileInputStream(RESOURCES.resolve("revXMLsDelete1").resolve("1.xml").toFile())) {
      new XmlShredder.Builder(holder.getXdmNodeWriteTrx(), XmlShredder.createFileReader(fis),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      final XmlNodeTrx wtx = holder.getXdmNodeWriteTrx();
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
  }

  static void setUpFourth(final Holder holder) throws IOException, XMLStreamException {
    initializeData(
        holder, RESOURCES.resolve("revXMLsAll3").resolve("1.xml"),
        RESOURCES.resolve("revXMLsAll3").resolve("2.xml"));
  }

  static void setUpFifth(final Holder holder)
      throws SirixException, IOException, XMLStreamException {
    initializeData(
        holder, RESOURCES.resolve("revXMLsAll2").resolve("1.xml"),
        RESOURCES.resolve("revXMLsAll2").resolve("2.xml"));
  }

  static void setUpSixth(final Holder holder)
      throws SirixException, IOException, XMLStreamException {
    initializeData(
        holder, RESOURCES.resolve("revXMLsDelete2").resolve("1.xml"),
        RESOURCES.resolve("revXMLsDelete2").resolve("2.xml"));
  }

  static void setUpSeventh(final Holder holder)
      {
    final XmlNodeTrx wtx = holder.getXdmNodeWriteTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    rtx.moveTo(1);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    wtx.commit();
    rtx.close();
  }

  static void setUpEighth(final Holder holder)
      {
    final XmlNodeTrx wtx = holder.getXdmNodeWriteTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    rtx.moveTo(11);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    wtx.commit();
    rtx.close();
  }

  private static void initializeData(final Holder holder, final Path... files)
      throws SirixException, IOException {

    int i = 0;
    for (final Path file : files) {
      try (final FileInputStream fis = new FileInputStream(file.toFile())) {
        if (i == 0) {
          final XmlShredder init = new XmlShredder.Builder(holder.getXdmNodeWriteTrx(),
              XmlShredder.createFileReader(fis), InsertPosition.AS_FIRST_CHILD).commitAfterwards().build();
          init.call();
        } else {
          final XMLUpdateShredder init = new XMLUpdateShredder(holder.getXdmNodeWriteTrx(),
              XmlShredder.createFileReader(fis), InsertPosition.AS_FIRST_CHILD, file, ShredderCommit.COMMIT);
          init.call();
        }
      }
      i++;
    }

  }

  static DiffObserver createMock() {
    return mock(DiffObserver.class);
  }

  static void verifyFullDiffFirst(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(3))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(2)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(10))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyOptimizedFullDiffFirst(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(3))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(2)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(4)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyStructuralDiffFirst(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(2)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(9))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyOptimizedStructuralDiffFirst(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(2)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(4)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffSecond(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.REPLACEDNEW), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(4))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyOptimizedSecond(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.REPLACEDNEW), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(3)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffThird(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(3)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyOptimizedThird(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(3)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffFourth(final DiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(3))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyOptimizedFourth(final DiffObserver pListener) {
    final InOrder inOrder = inOrder(pListener);
    inOrder.verify(pListener, times(3))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffListener(
        eq(DiffType.INSERTED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(2)).diffListener(
        eq(DiffType.SAMEHASH), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(pListener, times(1)).diffDone();
  }

  static void verifyDiffFifth(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.UPDATED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffSixth(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffListener(
        eq(DiffType.DELETED), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffSeventh(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(10)).diffListener(
        eq(DiffType.REPLACEDNEW), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(5))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void verifyDiffEighth(final DiffObserver listener) {
    final InOrder inOrder = inOrder(listener);
    inOrder.verify(listener, times(2))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(3)).diffListener(
        eq(DiffType.REPLACEDOLD), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(5))
           .diffListener(eq(DiffType.SAME), isA(Long.class), isA(Long.class), isA(DiffDepth.class));
    inOrder.verify(listener, times(1)).diffDone();
  }

  static void checkFullDiff(final Holder holder, final DiffObserver observer,
      final DiffOptimized optimized) {
    DiffFactory.invokeFullXmlDiff(
        new DiffFactory.Builder<>(holder.getResourceManager(), 2, 1, optimized,
            ImmutableSet.of(observer)));
  }

  static void checkStructuralDiff(final Holder holder, final DiffObserver observer,
      final DiffOptimized optimized) {
    DiffFactory.invokeStructuralXmlDiff(
        new DiffFactory.Builder<>(holder.getResourceManager(), 2, 1, optimized,
            ImmutableSet.of(observer)));
  }
}
