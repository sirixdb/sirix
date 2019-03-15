package org.sirix.xquery.function.jn;

import static org.sirix.xquery.function.jn.io.Doc.DOC;
import static org.sirix.xquery.function.jn.io.DocByPointInTime.OPEN;
import static org.sirix.xquery.function.jn.io.OpenRevisions.OPEN_REVISIONS;
import static org.sirix.xquery.function.sdb.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindCASIndex.FIND_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindNameIndex.FIND_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindPathIndex.FIND_PATH_INDEX;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.jn.index.create.CreateCASIndex;
import org.sirix.xquery.function.jn.index.create.CreateNameIndex;
import org.sirix.xquery.function.jn.index.create.CreatePathIndex;
import org.sirix.xquery.function.jn.index.find.FindCASIndex;
import org.sirix.xquery.function.jn.index.find.FindNameIndex;
import org.sirix.xquery.function.jn.index.find.FindPathIndex;
import org.sirix.xquery.function.jn.index.scan.ScanCASIndex;
import org.sirix.xquery.function.jn.index.scan.ScanCASIndexRange;
import org.sirix.xquery.function.jn.index.scan.ScanNameIndex;
import org.sirix.xquery.function.jn.index.scan.ScanPathIndex;
import org.sirix.xquery.function.jn.io.Doc;
import org.sirix.xquery.function.jn.io.DocByPointInTime;
import org.sirix.xquery.function.jn.io.OpenRevisions;
import org.sirix.xquery.function.jn.trx.SelectJsonItem;
import org.sirix.xquery.function.sdb.trx.SelectNode;

/**
 * Function definitions.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class JNFun {
  /** Prefix for Sirix json functions. */
  public static final String JN_PREFIX = "jn";

  /** Namespace URI for Sirix json functions. */
  public static final String JN_NSURI = "https://sirix.io/json";

  public static final QNm ERR_INVALID_ARGUMENT = new QNm(JN_NSURI, JN_PREFIX, "SIRIXDBF0001");

  public static final QNm ERR_INDEX_NOT_FOUND = new QNm(JN_NSURI, JN_PREFIX, "SIRIXDBF0002");

  public static final QNm ERR_FILE_NOT_FOUND = new QNm(JN_NSURI, JN_PREFIX, "SIRIXDBF0003");

  public static final QNm ERR_INVALID_INDEX_TYPE = new QNm(JN_NSURI, JN_PREFIX, "SIRIXDBF004");

  public static void register() {
    // dummy function to cause static block
    // to be executed exactly once
  }

  static {
    Namespaces.predefine(JNFun.JN_PREFIX, JNFun.JN_NSURI);

    // move to
    Functions.predefine(new SelectNode(SelectJsonItem.SELECT_JSON_ITEM, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.INT, Cardinality.One))));

    // // store
    // Functions.predefine(new Store(false));
    // Functions.predefine(new Store(true));
    // Functions.predefine(new Store(STORE, false));
    // Functions.predefine(new Store(STORE, true));
    //
    // // load
    // Functions.predefine(new Load(false));
    // Functions.predefine(new Load(true));
    // Functions.predefine(new Load(LOAD, false));
    // Functions.predefine(new Load(LOAD, true));

    // doc
    Functions.predefine(new Doc(DOC, new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
        new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(DOC,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(DOC, new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
        new SequenceType(AtomicType.STR, Cardinality.One))));

    // open
    Functions.predefine(new DocByPointInTime(OPEN,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne))));
    Functions.predefine(new DocByPointInTime(OPEN,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));

    // open-revisions
    Functions.predefine(new OpenRevisions(OPEN_REVISIONS,
        new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.STRING, SequenceType.STRING,
            new SequenceType(AtomicType.DATI, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.One))));

    // find-name-index
    Functions.predefine(new FindNameIndex(FIND_NAME_INDEX, new Signature(SequenceType.INTEGER, SequenceType.JSON_ITEM,
        new SequenceType(AtomicType.QNM, Cardinality.One))));

    // find-path-index
    Functions.predefine(new FindPathIndex(FIND_PATH_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.JSON_ITEM, SequenceType.STRING)));

    // find-cas-index
    Functions.predefine(new FindCASIndex(FIND_CAS_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.JSON_ITEM, SequenceType.STRING, SequenceType.STRING)));

    // create-name-index
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.QNM, Cardinality.ZeroOrMany))));
    Functions.predefine(
        new CreateNameIndex(CREATE_NAME_INDEX, new Signature(SequenceType.JSON_ITEM, SequenceType.JSON_ITEM)));

    // create-path-index
    Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(
        new CreatePathIndex(CREATE_PATH_INDEX, new Signature(SequenceType.JSON_ITEM, SequenceType.JSON_ITEM)));

    // create-cas-index
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX,
        new Signature(SequenceType.JSON_ITEM, SequenceType.JSON_ITEM,
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne))));
    Functions.predefine(
        new CreateCASIndex(CREATE_CAS_INDEX, new Signature(SequenceType.JSON_ITEM, SequenceType.JSON_ITEM)));

    // scan indexes
    Functions.predefine(new ScanPathIndex());
    Functions.predefine(new ScanCASIndex());
    Functions.predefine(new ScanCASIndexRange());
    Functions.predefine(new ScanNameIndex());
  }
}
