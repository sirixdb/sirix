package io.sirix.query.function.xml.io;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.access.Databases;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.diff.algorithm.fmse.DefaultNodeComparisonFactory;
import io.sirix.diff.algorithm.fmse.FMSE;
import io.sirix.diff.service.FMSEImport;
import io.sirix.utils.SirixFiles;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Function for importing the differences between the currently stored revision of a resource in a
 * collection/database and a new version of a resource. If successful, this function returns the
 * document-node. Supported signatures are:
 * </p>
 * <ul>
 * <li><code>xml:import($coll as xs:string, $res as xs:string, $resToImport as xs:string) as xs:node</code>
 * </li>
 * </ul>
 *
 * @author Max Bechtold
 * @author Johannes Lichtenberger
 *
 */
public final class Import extends AbstractFunction {

  /** Import function name. */
  public final static QNm IMPORT = new QNm(XMLFun.XML_NSURI, XMLFun.XML_PREFIX, "import");

  /**
   * Constructor.
   *
   * @param name the name of the function
   * @param signature the signature of the function
   */
  public Import(final QNm name, final Signature signature) {
    super(name, signature, true, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args) {
    if (args.length != 3) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final BasicXmlDBStore store = (BasicXmlDBStore) ctx.getNodeStore();
    final XmlDBCollection coll = store.lookup(((Str) args[0]).stringValue());

    if (coll == null) {
      throw new QueryException(new QNm("No valid arguments specified!"));
    }

    final String resName = ((Str) args[1]).stringValue();
    final String resToImport = ((Str) args[2]).stringValue();

    XmlDBNode doc = null;
    final XmlNodeReadOnlyTrx trx;

    try {
      doc = coll.getDocument(resName);

      try (final XmlNodeTrx wtx = doc.getTrx()
                                     .getResourceSession()
                                     .getNodeTrx()
                                     .orElse(doc.getTrx().getResourceSession().beginNodeTrx())) {
        final Path pathOfResToImport = Paths.get(resToImport);
        final Path newRevTarget = Files.createTempDirectory(pathOfResToImport.getFileName().toString());
        if (Files.exists(newRevTarget)) {
          SirixFiles.recursiveRemove(newRevTarget);
        }

        new FMSEImport().shredder(requireNonNull(pathOfResToImport), newRevTarget);

        try (final var databaseNew = Databases.openXmlDatabase(newRevTarget);
             final XmlResourceSession resourceNew = databaseNew.beginResourceSession("shredded");
             final XmlNodeReadOnlyTrx rtx = resourceNew.beginNodeReadOnlyTrx();
             final FMSE fmes = FMSE.createInstance(new DefaultNodeComparisonFactory())) {
          fmes.diff(wtx, rtx);
        }
      } catch (final IOException e) {
        throw new QueryException(new QNm("I/O exception: " + e.getMessage()), e);
      }
    } finally {
      trx = doc.getTrx().getResourceSession().beginNodeReadOnlyTrx();
      doc.getTrx().close();
    }

    return new XmlDBNode(trx, coll);
  }
}
