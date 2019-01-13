package org.sirix.fs;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import javax.annotation.Nonnull;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import com.google.common.base.Optional;

/**
 * Determines the operation to perform on sirix.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
@Nonnull
enum OperationType implements Operation<XdmNodeWriteTrx> {
  INSERT {
    @Override
    public void execute(final XdmNodeWriteTrx wtx, final Visitor<XdmNodeWriteTrx> visitor,
        final Map<Path, org.sirix.fs.FileSystemPath> index, final Path child)
        throws SirixException {
      checkNotNull(wtx);
      checkNotNull(child);
      checkNotNull(visitor);
      checkNotNull(index);
      checkArgument(
          wtx.getKind() == Kind.ELEMENT, "Transaction must be located at an element node!");
      if (Files.isDirectory(child)) {
        index.put(child, org.sirix.fs.FileSystemPath.ISDIRECTORY);
        wtx.insertElementAsFirstChild(new QNm("dir"));
      } else if (Files.isRegularFile(child) | Files.isSymbolicLink(child)) {
        index.put(child, org.sirix.fs.FileSystemPath.ISFILE);
        wtx.insertElementAsFirstChild(new QNm("file"));
      }
      wtx.insertAttribute(new QNm("name"), child.getFileName().toString());
      wtx.moveToParent();
      final long nodeKey = wtx.getNodeKey();
      processVisitor(visitor, wtx, child);
      wtx.moveTo(nodeKey);
    }
  },

  UPDATE {
    @Override
    public void execute(final XdmNodeWriteTrx wtx, final Visitor<XdmNodeWriteTrx> visitor,
        final Map<Path, org.sirix.fs.FileSystemPath> index, final Path child)
        throws SirixException {
      checkNotNull(wtx);
      checkNotNull(child);
      checkNotNull(visitor);
      checkNotNull(index);
      checkArgument(
          wtx.getKind() == Kind.ELEMENT, "Transaction must be located at an element node!");
      final long nodeKey = wtx.getNodeKey();
      processVisitor(visitor, wtx, child);
      wtx.moveTo(nodeKey);
    }
  },

  DELETE {
    @Override
    public void execute(final XdmNodeWriteTrx wtx, final Visitor<XdmNodeWriteTrx> visitor,
        final Map<Path, org.sirix.fs.FileSystemPath> index, final Path child)
        throws SirixException {
      checkNotNull(wtx);
      checkNotNull(child);
      checkNotNull(visitor);
      checkNotNull(index);
      checkArgument(
          wtx.getKind() == Kind.ELEMENT, "Transaction must be located at an element node!");
      wtx.remove();
    }
  };

  /**
   * Process changed {@link Path} with an optional visitor if present.
   *
   * @param visitor optional visitor
   * @param wtx sirix {@link XdmNodeWriteTrx}
   * @param path {@link Path} reference
   */
  private static void processVisitor(Visitor<XdmNodeWriteTrx> visitor, final XdmNodeWriteTrx wtx,
      final Path path) {
    assert path != null;
    if (visitor != null) {
      if (Files.isDirectory(path)) {
        visitor.processDirectory(wtx, path, Optional.<BasicFileAttributes>absent());
      } else if (Files.isRegularFile(path) | Files.isSymbolicLink(path)) {
        visitor.processFile(wtx, path, Optional.<BasicFileAttributes>absent());
      }
    }
  }
}
