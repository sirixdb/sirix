package org.sirix.fs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;

/**
 * Determines the operation to perform on sirix.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
enum EOperation implements IOperation<INodeWriteTrx> {
  INSERT {
    @Override
    public void execute(final INodeWriteTrx pWtx, final Optional<IVisitor<INodeWriteTrx>> pVisitor,
      final Map<Path, EPath> pIndex, final Path pChild) throws SirixException {
      checkNotNull(pWtx);
      checkNotNull(pChild);
      checkNotNull(pVisitor);
      checkNotNull(pIndex);
      checkArgument(pWtx.getKind() == EKind.ELEMENT,
        "Transaction must be located at an element node!");
      if (Files.isDirectory(pChild)) {
        pIndex.put(pChild, EPath.ISDIRECTORY);
        pWtx.insertElementAsFirstChild(new QName("dir"));
      } else if (Files.isRegularFile(pChild) | Files.isSymbolicLink(pChild)) {
        pIndex.put(pChild, EPath.ISFILE);
        pWtx.insertElementAsFirstChild(new QName("file"));
      }
      pWtx.insertAttribute(new QName("name"), pChild.getFileName().toString());
      pWtx.moveToParent();
      final long nodeKey = pWtx.getNodeKey();
      processVisitor(pVisitor, pWtx, pChild);
      pWtx.moveTo(nodeKey);
    }
  },

  UPDATE {
    @Override
    public void execute(final INodeWriteTrx pWtx, final Optional<IVisitor<INodeWriteTrx>> pVisitor,
      final Map<Path, EPath> pIndex, final Path pChild) throws SirixException {
      checkNotNull(pWtx);
      checkNotNull(pChild);
      checkNotNull(pVisitor);
      checkNotNull(pIndex);
      checkArgument(pWtx.getKind() == EKind.ELEMENT,
        "Transaction must be located at an element node!");
      final long nodeKey = pWtx.getNodeKey();
      processVisitor(pVisitor, pWtx, pChild);
      pWtx.moveTo(nodeKey);
    }
  },

  DELETE {
    @Override
    public void execute(final INodeWriteTrx pWtx, final Optional<IVisitor<INodeWriteTrx>> pVisitor,
      final Map<Path, EPath> pIndex, final Path pChild) throws SirixException {
      checkNotNull(pWtx);
      checkNotNull(pChild);
      checkNotNull(pVisitor);
      checkNotNull(pIndex);
      checkArgument(pWtx.getKind() == EKind.ELEMENT,
        "Transaction must be located at an element node!");
      pWtx.remove();
    }
  };

  /**
   * Process changed {@link Path} with an optional visitor if present.
   * 
   * @param pVisitor
   *          optional visitor
   * @param pWtx
   *          sirix {@link INodeWriteTrx}
   * @param pPath
   *          {@link Path} reference
   */
  private static void processVisitor(Optional<IVisitor<INodeWriteTrx>> pVisitor, final INodeWriteTrx pWtx,
    final Path pPath) {
    assert pVisitor != null;
    assert pPath != null;
    if (pVisitor.isPresent()) {
      if (Files.isDirectory(pPath)) {
        pVisitor.get().processDirectory(pWtx, pPath, Optional.<BasicFileAttributes> absent());
      } else if (Files.isRegularFile(pPath) | Files.isSymbolicLink(pPath)) {
        pVisitor.get().processFile(pWtx, pPath, Optional.<BasicFileAttributes> absent());
      }
    }
  }
}
