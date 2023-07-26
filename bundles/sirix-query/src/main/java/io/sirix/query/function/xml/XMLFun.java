package io.sirix.query.function.xml;

import io.sirix.query.function.sdb.trx.LevelOrder;
import io.sirix.query.function.xml.diff.Diff;
import io.sirix.query.function.xml.index.SortByDocOrder;
import io.sirix.query.function.xml.index.create.CreateCASIndex;
import io.sirix.query.function.xml.index.create.CreateNameIndex;
import io.sirix.query.function.xml.index.create.CreatePathIndex;
import io.sirix.query.function.xml.index.find.FindCASIndex;
import io.sirix.query.function.xml.index.find.FindNameIndex;
import io.sirix.query.function.xml.index.find.FindPathIndex;
import io.sirix.query.function.xml.index.scan.ScanCASIndex;
import io.sirix.query.function.xml.index.scan.ScanCASIndexRange;
import io.sirix.query.function.xml.index.scan.ScanNameIndex;
import io.sirix.query.function.xml.index.scan.ScanPathIndex;
import io.sirix.query.function.xml.io.*;
import io.sirix.query.function.xml.trx.GetAttributeCount;
import io.sirix.query.function.xml.trx.GetNamespaceCount;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.type.AnyJsonItemType;
import org.brackit.xquery.jdm.type.AtomicType;
import org.brackit.xquery.jdm.type.Cardinality;
import org.brackit.xquery.jdm.type.SequenceType;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;

import static io.sirix.query.function.xml.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static io.sirix.query.function.xml.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static io.sirix.query.function.xml.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static io.sirix.query.function.xml.index.find.FindCASIndex.FIND_CAS_INDEX;
import static io.sirix.query.function.xml.index.find.FindNameIndex.FIND_NAME_INDEX;
import static io.sirix.query.function.xml.index.find.FindPathIndex.FIND_PATH_INDEX;

/**
 * Function definitions.
 *
 * @author Johannes Lichtenberger
 */
public final class XMLFun {
  /**
   * Prefix for Sirix functions.
   */
  public static final String XML_PREFIX = "xml";

  /**
   * Namespace URI for Sirix functions.
   */
  public static final String XML_NSURI = "https://sirix.io/xml";

  public static final QNm ERR_INDEX_NOT_FOUND = new QNm(XML_NSURI, XML_PREFIX, "SIRIXDBF0001");

  public static final QNm ERR_INVALID_INDEX_TYPE = new QNm(XML_NSURI, XML_PREFIX, "SIRIXDBF002");

  public static void register() {
    // dummy function to cause static block
    // to be executed exactly once
  }

  static {
    Namespaces.predefine(XMLFun.XML_PREFIX, XMLFun.XML_NSURI);

    // sort by document order
    Functions.predefine(new SortByDocOrder(SortByDocOrder.SORT,
                                           new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM_SEQUENCE)));

    // store
    Functions.predefine(new Store(false));
    Functions.predefine(new Store(true));
    Functions.predefine(new Store(Store.STORE, false));
    Functions.predefine(new Store(Store.STORE, true));
    Functions.predefine(new Store(Store.STORE,
                                  new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.DATI, Cardinality.One))));
    Functions.predefine(new Store(Store.STORE,
                                  new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One))));

    // load
    Functions.predefine(new Load(false));
    Functions.predefine(new Load(true));
    Functions.predefine(new Load(Load.LOAD, false));
    Functions.predefine(new Load(Load.LOAD, true));
    Functions.predefine(new Load(Load.LOAD,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.DATI, Cardinality.One))));
    Functions.predefine(new Load(Load.LOAD,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One))));

    // doc
    Functions.predefine(new Doc(Doc.DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(Doc.DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne),
                                              new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(Doc.DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One))));

    // open
    Functions.predefine(new DocByPointInTime(DocByPointInTime.OPEN,
                                             new Signature(SequenceType.NODE,
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne))));
    Functions.predefine(new DocByPointInTime(DocByPointInTime.OPEN,
                                             new Signature(SequenceType.NODE,
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne),
                                                           new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));

    // open-revisions
    Functions.predefine(new OpenRevisions(OpenRevisions.OPEN_REVISIONS,
                                          new Signature(SequenceType.ITEM_SEQUENCE,
                                                        SequenceType.STRING,
                                                        SequenceType.STRING,
                                                        new SequenceType(AtomicType.DATI, Cardinality.One),
                                                        new SequenceType(AtomicType.DATI, Cardinality.One))));

    // level-order
    Functions.predefine(new LevelOrder(LevelOrder.LEVEL_ORDER,
                                       new Signature(SequenceType.ITEM_SEQUENCE,
                                                     SequenceType.NODE,
                                                     new SequenceType(AtomicType.INT, Cardinality.One))));
    Functions.predefine(new LevelOrder(LevelOrder.LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.NODE)));

    // get-namespace-count
    Functions.predefine(new GetNamespaceCount(GetNamespaceCount.GET_NAMESPACE_COUNT,
                                              new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-attribute-count
    Functions.predefine(new GetAttributeCount(GetAttributeCount.GET_ATTRIBUTE_COUNT,
                                              new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // find-name-index
    Functions.predefine(new FindNameIndex(FIND_NAME_INDEX,
                                          new Signature(SequenceType.INTEGER,
                                                        SequenceType.NODE,
                                                        new SequenceType(AtomicType.QNM, Cardinality.One))));

    // find-path-index
    Functions.predefine(new FindPathIndex(FIND_PATH_INDEX,
                                          new Signature(SequenceType.INTEGER, SequenceType.NODE, SequenceType.STRING)));

    // find-cas-index
    Functions.predefine(new FindCASIndex(FIND_CAS_INDEX,
                                         new Signature(SequenceType.INTEGER,
                                                       SequenceType.NODE,
                                                       SequenceType.STRING,
                                                       SequenceType.STRING)));

    // create-name-index
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX,
                                            new Signature(SequenceType.NODE,
                                                          SequenceType.NODE,
                                                          new SequenceType(AtomicType.QNM, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // create-path-index
    Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX,
                                            new Signature(SequenceType.NODE,
                                                          SequenceType.NODE,
                                                          new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // create-cas-index
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX,
                                           new Signature(SequenceType.NODE,
                                                         SequenceType.NODE,
                                                         new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                                         new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX,
                                           new Signature(SequenceType.NODE,
                                                         SequenceType.NODE,
                                                         new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne))));
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // scan indexes
    Functions.predefine(new ScanPathIndex());
    Functions.predefine(new ScanCASIndex());
    Functions.predefine(new ScanCASIndexRange());
    Functions.predefine(new ScanNameIndex());

    // diff
    Functions.predefine(new Diff(Diff.DIFF,
                                 new Signature(SequenceType.STRING,
                                               SequenceType.STRING,
                                               SequenceType.STRING,
                                               SequenceType.INTEGER,
                                               SequenceType.INTEGER)));

    // import
    Functions.predefine(new Import(Import.IMPORT,
                                   new Signature(SequenceType.NODE,
                                                 new SequenceType(AtomicType.STR, Cardinality.One),
                                                 new SequenceType(AtomicType.STR, Cardinality.One),
                                                 new SequenceType(AtomicType.STR, Cardinality.One))));
  }
}
