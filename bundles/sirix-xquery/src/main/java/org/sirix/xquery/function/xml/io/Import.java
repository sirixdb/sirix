package org.sirix.xquery.function.xml.io;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.function.AbstractFunction;
import org.brackit.xquery.jdm.Sequence;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.module.StaticContext;
import org.sirix.access.Databases;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.diff.algorithm.fmse.DefaultNodeComparisonFactory;
import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.diff.service.FMSEImport;
import org.sirix.utils.SirixFiles;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

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
