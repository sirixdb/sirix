package org.sirix.xquery.function.xml;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.jdm.Signature;
import org.brackit.xquery.jdm.type.AnyJsonItemType;
import org.brackit.xquery.jdm.type.AtomicType;
import org.brackit.xquery.jdm.type.Cardinality;
import org.brackit.xquery.jdm.type.SequenceType;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.sirix.xquery.function.sdb.trx.LevelOrder;
import org.sirix.xquery.function.xml.diff.Diff;
import org.sirix.xquery.function.xml.index.SortByDocOrder;
import org.sirix.xquery.function.xml.index.create.CreateCASIndex;
import org.sirix.xquery.function.xml.index.create.CreateNameIndex;
import org.sirix.xquery.function.xml.index.create.CreatePathIndex;
import org.sirix.xquery.function.xml.index.find.FindCASIndex;
import org.sirix.xquery.function.xml.index.find.FindNameIndex;
import org.sirix.xquery.function.xml.index.find.FindPathIndex;
import org.sirix.xquery.function.xml.index.scan.ScanCASIndex;
import org.sirix.xquery.function.xml.index.scan.ScanCASIndexRange;
import org.sirix.xquery.function.xml.index.scan.ScanNameIndex;
import org.sirix.xquery.function.xml.index.scan.ScanPathIndex;
import org.sirix.xquery.function.xml.io.*;
import org.sirix.xquery.function.xml.trx.GetAttributeCount;
import org.sirix.xquery.function.xml.trx.GetNamespaceCount;

import static org.sirix.xquery.function.sdb.trx.LevelOrder.LEVEL_ORDER;
import static org.sirix.xquery.function.xml.diff.Diff.DIFF;
import static org.sirix.xquery.function.xml.index.SortByDocOrder.SORT;
import static org.sirix.xquery.function.xml.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.xml.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static org.sirix.xquery.function.xml.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static org.sirix.xquery.function.xml.index.find.FindCASIndex.FIND_CAS_INDEX;
import static org.sirix.xquery.function.xml.index.find.FindNameIndex.FIND_NAME_INDEX;
import static org.sirix.xquery.function.xml.index.find.FindPathIndex.FIND_PATH_INDEX;
import static org.sirix.xquery.function.xml.io.Doc.DOC;
import static org.sirix.xquery.function.xml.io.DocByPointInTime.OPEN;
import static org.sirix.xquery.function.xml.io.Import.IMPORT;
import static org.sirix.xquery.function.xml.io.Load.LOAD;
import static org.sirix.xquery.function.xml.io.OpenRevisions.OPEN_REVISIONS;
import static org.sirix.xquery.function.xml.io.Store.STORE;
import static org.sirix.xquery.function.xml.trx.GetAttributeCount.GET_ATTRIBUTE_COUNT;
import static org.sirix.xquery.function.xml.trx.GetNamespaceCount.GET_NAMESPACE_COUNT;

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
    Functions.predefine(new SortByDocOrder(SORT,
                                           new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM_SEQUENCE)));

    // store
    Functions.predefine(new Store(false));
    Functions.predefine(new Store(true));
    Functions.predefine(new Store(STORE, false));
    Functions.predefine(new Store(STORE, true));
    Functions.predefine(new Store(STORE,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.DATI, Cardinality.One))));
    Functions.predefine(new Store(STORE,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One))));

    // load
    Functions.predefine(new Load(false));
    Functions.predefine(new Load(true));
    Functions.predefine(new Load(LOAD, false));
    Functions.predefine(new Load(LOAD, true));
    Functions.predefine(new Load(LOAD,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.DATI, Cardinality.One))));
    Functions.predefine(new Load(LOAD,
                                 new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
                                               new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany),
                                               new SequenceType(AtomicType.BOOL, Cardinality.One),
                                               new SequenceType(AtomicType.STR, Cardinality.One))));

    // doc
    Functions.predefine(new Doc(DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne),
                                              new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(DOC,
                                new Signature(SequenceType.NODE,
                                              new SequenceType(AtomicType.STR, Cardinality.One),
                                              new SequenceType(AtomicType.STR, Cardinality.One))));

    // open
    Functions.predefine(new DocByPointInTime(OPEN,
                                             new Signature(SequenceType.NODE,
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne))));
    Functions.predefine(new DocByPointInTime(OPEN,
                                             new Signature(SequenceType.NODE,
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.STR, Cardinality.One),
                                                           new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne),
                                                           new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));

    // open-revisions
    Functions.predefine(new OpenRevisions(OPEN_REVISIONS,
                                          new Signature(SequenceType.ITEM_SEQUENCE,
                                                        SequenceType.STRING,
                                                        SequenceType.STRING,
                                                        new SequenceType(AtomicType.DATI, Cardinality.One),
                                                        new SequenceType(AtomicType.DATI, Cardinality.One))));

    // level-order
    Functions.predefine(new LevelOrder(LEVEL_ORDER,
                                       new Signature(SequenceType.ITEM_SEQUENCE,
                                                     SequenceType.NODE,
                                                     new SequenceType(AtomicType.INT, Cardinality.One))));
    Functions.predefine(new LevelOrder(LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.NODE)));

    // get-namespace-count
    Functions.predefine(new GetNamespaceCount(GET_NAMESPACE_COUNT,
                                              new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-attribute-count
    Functions.predefine(new GetAttributeCount(GET_ATTRIBUTE_COUNT,
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
    Functions.predefine(new Diff(DIFF,
                                 new Signature(SequenceType.STRING,
                                               SequenceType.STRING,
                                               SequenceType.STRING,
                                               SequenceType.INTEGER,
                                               SequenceType.INTEGER)));

    // import
    Functions.predefine(new Import(IMPORT,
                                   new Signature(SequenceType.NODE,
                                                 new SequenceType(AtomicType.STR, Cardinality.One),
                                                 new SequenceType(AtomicType.STR, Cardinality.One),
                                                 new SequenceType(AtomicType.STR, Cardinality.One))));
  }
}
