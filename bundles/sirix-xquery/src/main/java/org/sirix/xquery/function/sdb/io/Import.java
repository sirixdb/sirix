package org.sirix.xquery.function.sdb.io;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.xml.stream.XMLStreamException;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.xdm.Sequence;
import org.brackit.xquery.xdm.Signature;
import org.sirix.access.Databases;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.diff.service.FMSEImport;
import org.sirix.utils.SirixFiles;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.BasicDBStore;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;

/**
 * <p>
 * Function for importing the differences between the currently stored revision of a resource in a
 * collection/database and a new version of a resource. If successful, this function returns the
 * document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>sdb:import($coll as xs:string, $res as xs:string, $resToImport as xs:string) as xs:node</code>
 * </li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class Import extends AbstractFunction {

  /** Import function name. */
  public final static QNm IMPORT = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "import");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Import(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    if (args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final BasicDBStore store = (BasicDBStore) ctx.getStore();
    final DBCollection coll = store.lookup(((Str) args[0]).stringValue());

    if (coll == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String resName = ((Str) args[1]).stringValue();
    final String resToImport = ((Str) args[2]).stringValue();

    DBNode doc = null;
    final XdmNodeReadOnlyTrx trx;

    try {
      doc = coll.getDocument(resName);

      try (final XdmNodeTrx wtx = doc.getTrx()
                                          .getResourceManager()
                                          .getNodeWriteTrx()
                                          .orElse(doc.getTrx().getResourceManager().beginNodeTrx())) {
        final Path newRevTarget = Files.createTempDirectory(Paths.get(resToImport).getFileName().toString());
        if (Files.exists(newRevTarget)) {
          SirixFiles.recursiveRemove(newRevTarget);
        }
        try {
          FMSEImport.shredder(checkNotNull(Paths.get(resToImport)), newRevTarget);
        } catch (final XMLStreamException e) {
          throw new QueryException(new QNm("XML stream exception: " + e.getMessage()), e);
        }

        try (final var databaseNew = Databases.openXdmDatabase(newRevTarget);
            final XdmResourceManager resourceNew = databaseNew.getResourceManager("shredded");
            final XdmNodeReadOnlyTrx rtx = resourceNew.beginNodeReadOnlyTrx();
            final FMSE fmes = new FMSE()) {
          fmes.diff(wtx, rtx);
        }
      } catch (final IOException e) {
        throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
      }
    } finally {
      trx = doc.getTrx().getResourceManager().beginNodeReadOnlyTrx();
      doc.getTrx().close();
    }

    return new DBNode(trx, coll);
  }
}
