package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Optional;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.fs.HierarchyFileVisitor.Builder;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.Session;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;

/**
 * Parses a directory in the file system and creates a sirix {@IDatabase} with an initial
 * revision.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
public class FileHierarchyWalker {

  /**
   * Parse a directory and create a simple XML representation.
   * 
   * @param pPath
   *          path to directory from which to shredder all content into sirix
   * @param pDatabase
   *          sirix {@IDatabase} to shred into
   * @param pVisitor
   *          an optional visitor
   * @throws SirixException
   *           if any sirix operation fails
   * @throws IOException
   *           if an I/O error occurs
   * @throws NullPointerException
   *           if one of the arguments is {@code null}
   */
  public static Map<Path, org.sirix.fs.Path> parseDir(final Path pPath, final Database pDatabase,
    Optional<Visitor<NodeWriteTrx>> pVisitor) throws SirixException, IOException {
    checkNotNull(pVisitor);
    final Path path = checkNotNull(pPath);
    final Session session =
      checkNotNull(pDatabase).getSession(new SessionConfiguration.Builder("shredded").build());
    final NodeWriteTrx wtx = session.beginNodeWriteTrx();
    final Builder builder = new Builder(wtx);
    if (pVisitor.isPresent()) {
      builder.setVisitor(pVisitor.get());
    }
    Map<Path, org.sirix.fs.Path> index = Collections.emptyMap();
    try (final HierarchyFileVisitor visitor = HierarchyFileVisitor.getInstance(builder)) {
      Files.walkFileTree(path, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, visitor);
      index = visitor.getIndex();
    }
    wtx.close();
    session.close();
    return index;
  }
}
