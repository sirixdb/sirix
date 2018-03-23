package org.sirix.fs;

import java.nio.file.Path;
import java.util.Map;
import javax.annotation.Nonnull;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.exception.SirixException;

/**
 * Interface for operations for usage with the command pattern.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 * @param <S> first argument for the operation, the sirix transaction which is also a generic
 *        parameter for the visitor
 */
@Nonnull
public interface Operation<S extends XdmNodeReadTrx> {

  /**
   * Execute an operation.
   *
   * @param transaction the sirix transaction
   * @param visitor a visitor which can be used to plugin further metadata
   * @param index simple index structure
   * @param child the {@link Path} instance for which a new node element should be inserted into
   *        sirix
   * @throws SirixException if any operation in sirix fails
   */
  void execute(S transaction, Visitor<S> visitor, Map<Path, org.sirix.fs.FileSystemPath> index,
      Path child) throws SirixException;
}
