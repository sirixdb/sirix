package org.sirix.fs;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.AbsShredder;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * Implements the {@link FileVisitor} interface and shredders an XML representation of directories/files into
 * sirix. The XML representation can be easily equipped with further functionality through the usage of the
 * "Execute Around" idiom.
 * 
 * Further functionality can be plugged in through the implementation of the {@link IVisitor} interface.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
public class HierarchyFileVisitor extends AbsShredder implements AutoCloseable, FileVisitor<Path> {

  /**
   * Mapping of {@link IDatabase} to {@link HierarchyFileVisitor} shared among all
   * instances.
   */
  private static final ConcurrentMap<INodeWriteTrx, HierarchyFileVisitor> INSTANCES =
    new ConcurrentHashMap<>();

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(HierarchyFileVisitor.class));

  /** sirix {@link INodeWriteTrx}. */
  private final INodeWriteTrx mWtx;

  /** Visitor which simply can be plugged in to create a more thorough XML representation. */
  private final Optional<IVisitor<INodeWriteTrx>> mVisitor;

  /** Index entries {@code String} representation to {@link EPath} value. */
  private final Map<Path, EPath> mIndex;

  /** Simple Builder. */
  public static class Builder {

    /** sirix {@link INodeWriteTrx}. */
    private final INodeWriteTrx mWtx;

    /** Implementation of the {@link IVisitor} interface. */
    private Optional<IVisitor<INodeWriteTrx>> mVisitor = Optional.absent();

    /**
     * Constructor.
     * 
     * @param pDatabase
     *          sirix {@link INodeWriteTrx}
     */
    public Builder(final INodeWriteTrx pWtx) {
      mWtx = checkNotNull(pWtx);
    }

    /**
     * Set an {@link IVisitor} implementation.
     * 
     * @param pVisitor
     *          {@link IVisitor} implementation
     * @return this builder instance
     */
    public Builder setVisitor(final IVisitor<INodeWriteTrx> pVisitor) {
      mVisitor = Optional.fromNullable(pVisitor);
      return this;
    }

    /**
     * Build a new {@link HierarchyFileVisitor} instance.
     * 
     * @return {@link HierarchyFileVisitor} reference
     * @throws SirixException
     *           if setting up sirix fails
     */
    public HierarchyFileVisitor build() throws SirixException {
      return new HierarchyFileVisitor(this);
    }
  }

  /**
   * Private constructor invoked from factory.
   * 
   * @param pPath
   *          {@link Path} reference which denotes the {@code path/directory} to watch for changes.
   * @param pDatabase
   *          {@link IDatabase} to use for importing changed data into sirix
   * @throws NullPointerException
   *           if {@code pBuilder} is {@code null}
   */
  private HierarchyFileVisitor(final Builder pBuilder) throws SirixException {
    super(pBuilder.mWtx, EInsert.ASFIRSTCHILD);
    mVisitor = pBuilder.mVisitor;
    mWtx = pBuilder.mWtx;
    mWtx.insertElementAsFirstChild(new QName("fsml"));
    mIndex = Maps.newHashMap();
  }

  /**
   * Get an instance of {@link FileHierarchyWalker}. If an instance with the specified {@code {@link Path}/
   * {@link IDatabase} already exists this instance is returned.
   * 
   * @param pPath
   *          {@link Path} reference which denotes the {@code path/directory} to watch for changes.
   * @return a new {@link FileHierarchyWalker} instance
   * @throws NullPointerException
   *           if {@code pBuilder} is {@code null}
   * @throws SirixException
   *           if anything while setting up sirix failes
   */
  public static synchronized HierarchyFileVisitor getInstance(final Builder pBuilder) throws SirixException {
    HierarchyFileVisitor visitor = INSTANCES.putIfAbsent(pBuilder.mWtx, pBuilder.build());
    if (visitor == null) {
      visitor = INSTANCES.get(pBuilder.mWtx);
    }
    return visitor;
  }

  /**
   * <p>
   * Invoked for a directory before directory contents are visited.
   * </p>
   * <p>
   * Inserts a {@code dir-element} with a {@code name-attribute} for the directory to visit.
   * </p>
   * <p>
   * An optional visitor can be used to add further attributes or metadata. The sirix {@link INodeWriteTrx}
   * is located on the new directory before and after using a pluggable visitor.
   * </p>
   * 
   * @throws NullPointerException
   *           if {@code pDir} or {@code pAttrs} is {@code null}
   */
  @Override
  public FileVisitResult preVisitDirectory(final Path pDir, final BasicFileAttributes pAttrs)
    throws IOException {
    checkNotNull(pDir);
    checkNotNull(pAttrs);
    try {
      mIndex.put(pDir, EPath.ISDIRECTORY);
      processStartTag(new QName("dir"));
      mWtx.insertAttribute(new QName("name"), pDir.getFileName().toString());
      mWtx.moveToParent();
      final long nodeKey = mWtx.getNode().getNodeKey();
      processDirectory(mVisitor, mWtx, pDir, pAttrs);
      mWtx.moveTo(nodeKey);
    } catch (SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult postVisitDirectory(final Path pDir, final IOException pExc) throws IOException {
    checkNotNull(pDir);
    if (pExc != null) {
      LOGWRAPPER.debug(pExc.getMessage(), pExc);
    }
    processEndTag(new QName(""));
    return FileVisitResult.CONTINUE;
  }

  /**
   * <p>
   * Inserts a {@code file-element} with a {@code name-attribute} for the file which is visited.
   * </p>
   * <p>
   * An optional visitor can be used to add further attributes or metadata. The sirix {@link INodeWriteTrx}
   * is located on the new directory before and after using a pluggable visitor.
   * </p>
   * 
   * @throws NullPointerException
   *           if {@code pDir} or {@code pAttrs} is {@code null}
   */
  @Override
  public FileVisitResult visitFile(final Path pFile, final BasicFileAttributes pAttrs) throws IOException {
    checkNotNull(pFile);
    checkNotNull(pAttrs);
    try {
      if (Files.isRegularFile(pFile) | Files.isSymbolicLink(pFile)) {
        mIndex.put(pFile, EPath.ISFILE);
        processEmptyElement(new QName("file"));
        mWtx.insertAttribute(new QName("name"), pFile.getFileName().toString());
        mWtx.moveToParent();
        final long nodeKey = mWtx.getNode().getNodeKey();
        processFile(mVisitor, mWtx, pFile, pAttrs);
        mWtx.moveTo(nodeKey);
      }
    } catch (SirixException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return FileVisitResult.CONTINUE;
  }

  /**
   * Process a directory.
   * 
   * @param pVisitor
   *          an optional visitor implementing {@link IVisitor}
   * @param pWtx
   *          sirix {@link INodeWriteTrx}
   * @see IVisitor#processDirectory(INodeReadTrx) processDirectory(IReadTransaction)
   */
  private void processDirectory(final Optional<IVisitor<INodeWriteTrx>> pVisitor, final INodeWriteTrx pWtx,
    final Path pDir, final BasicFileAttributes pAttrs) {
    assert pVisitor != null;
    assert pWtx != null;
    if (pVisitor.isPresent()) {
      pVisitor.get().processDirectory(pWtx, pDir, Optional.of(pAttrs));
    }
  }

  /**
   * Process a file.
   * 
   * @param pVisitor
   *          an optional visitor implementing {@link IVisitor}
   * @param pWtx
   *          sirix {@link INodeWriteTrx}
   * @see IVisitor#processFile(INodeReadTrx) processFile(IReadTransaction)
   */
  private void processFile(final Optional<IVisitor<INodeWriteTrx>> pVisitor, final INodeWriteTrx pWtx,
    final Path pFile, final BasicFileAttributes pAttrs) {
    assert pVisitor != null;
    assert pWtx != null;
    if (pVisitor.isPresent()) {
      pVisitor.get().processFile(pWtx, pFile, Optional.of(pAttrs));
    }
  }

  @Override
  public void close() throws SirixException {
    mWtx.commit();
    mWtx.close();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("instances", INSTANCES).add("wtx", mWtx).toString();
  }

  /**
   * Get the path index.
   * 
   * @return path index
   */
  public Map<Path, EPath> getIndex() {
    return mIndex;
  }

  @Override
  public FileVisitResult visitFileFailed(final Path pFile, final IOException pExc) throws IOException {
    LOGWRAPPER.error(pExc.getMessage(), pExc);
    return FileVisitResult.CONTINUE;
  }
}
