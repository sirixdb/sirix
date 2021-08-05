package org.sirix.xquery.function.jn;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.function.json.JSONFun;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AnyJsonItemType;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.jn.diff.Diff;
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
import org.sirix.xquery.function.jn.io.*;
import org.sirix.xquery.function.jn.temporal.*;
import org.sirix.xquery.function.jn.trx.SelectJsonItem;

import static org.sirix.xquery.function.jn.diff.Diff.DIFF;
import static org.sirix.xquery.function.jn.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.jn.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static org.sirix.xquery.function.jn.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static org.sirix.xquery.function.jn.index.find.FindCASIndex.FIND_CAS_INDEX;
import static org.sirix.xquery.function.jn.index.find.FindNameIndex.FIND_NAME_INDEX;
import static org.sirix.xquery.function.jn.index.find.FindPathIndex.FIND_PATH_INDEX;
import static org.sirix.xquery.function.jn.io.Doc.DOC;
import static org.sirix.xquery.function.jn.io.DocByPointInTime.OPEN;
import static org.sirix.xquery.function.jn.io.Load.LOAD;
import static org.sirix.xquery.function.jn.io.OpenRevisions.OPEN_REVISIONS;
import static org.sirix.xquery.function.jn.io.Store.STORE;


/**
 * Function definitions.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class JNFun {
  public static final QNm ERR_INVALID_ARGUMENT = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "SIRIXDBF0001");

  public static final QNm ERR_INDEX_NOT_FOUND = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "SIRIXDBF0002");

  public static final QNm ERR_FILE_NOT_FOUND = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "SIRIXDBF0003");

  public static final QNm ERR_INVALID_INDEX_TYPE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "SIRIXDBF004");

  public static void register() {
    // dummy function to cause static block
    // to be executed exactly once
  }

  static {
    // move to
    Functions.predefine(new SelectJsonItem(SelectJsonItem.SELECT_JSON_ITEM, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.INT, Cardinality.One))));

    // temporal functions
    Functions.predefine(
        new Future(Future.FUTURE, new Signature(SequenceType.JSON_ITEM_SEQUENCE, SequenceType.JSON_ITEM)));
    Functions.predefine(new Future(Future.FUTURE, new Signature(SequenceType.JSON_ITEM_SEQUENCE, SequenceType.JSON_ITEM,
        new SequenceType(AtomicType.BOOL, Cardinality.One))));
    Functions.predefine(new Past(Past.PAST, new Signature(SequenceType.JSON_ITEM_SEQUENCE, SequenceType.JSON_ITEM)));
    Functions.predefine(new Past(Past.PAST, new Signature(SequenceType.JSON_ITEM_SEQUENCE, SequenceType.JSON_ITEM,
        new SequenceType(AtomicType.BOOL, Cardinality.One))));
    Functions.predefine(new Next(Next.NEXT,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(new Previous(Previous.PREVIOUS,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(new First(First.FIRST,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(new FirstExisting(FirstExisting.FIRST_EXISTING,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(new Last(Last.LAST,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(new LastExisting(LastExisting.LAST_EXISTING,
        new Signature(new SequenceType(AnyJsonItemType.ANY_JSON_ITEM, Cardinality.ZeroOrOne), SequenceType.JSON_ITEM)));
    Functions.predefine(
        new AllTimes(AllTimes.ALL_TIMES, new Signature(SequenceType.JSON_ITEM_SEQUENCE, SequenceType.JSON_ITEM)));

    // store
    Functions.predefine(new Store(false));
    Functions.predefine(new Store(true));
    Functions.predefine(new Store(STORE, false));
    Functions.predefine(new Store(STORE, true));

    // load
    Functions.predefine(new Load(false));
    Functions.predefine(new Load(true));
    Functions.predefine(new Load(LOAD, false));
    Functions.predefine(new Load(LOAD, true));

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
        new SequenceType(AtomicType.STR, Cardinality.One))));

    // find-path-index
    Functions.predefine(new FindPathIndex(FIND_PATH_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.JSON_ITEM, SequenceType.STRING)));

    // find-cas-index
    Functions.predefine(new FindCASIndex(FIND_CAS_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.JSON_ITEM, SequenceType.STRING, SequenceType.STRING)));

    // create-name-index
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(SequenceType.JSON_ITEM,
        SequenceType.JSON_ITEM, new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
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

    // diff
    Functions.predefine(new Diff(DIFF, new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING,
                                                     SequenceType.INTEGER, SequenceType.INTEGER)));
    Functions.predefine(new Diff(DIFF, new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING,
        SequenceType.INTEGER, SequenceType.INTEGER, SequenceType.INTEGER)));
    Functions.predefine(new Diff(DIFF, new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING,
        SequenceType.INTEGER, SequenceType.INTEGER, SequenceType.INTEGER, SequenceType.INTEGER)));
  }
}
