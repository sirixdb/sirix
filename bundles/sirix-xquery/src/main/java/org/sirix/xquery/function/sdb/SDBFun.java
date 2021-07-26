package org.sirix.xquery.function.sdb;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.Type;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.sdb.trx.*;
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
import org.sirix.xquery.function.xml.trx.GetNamespaceCount;

import static org.sirix.xquery.function.sdb.trx.Commit.COMMIT;
import static org.sirix.xquery.function.sdb.trx.GetAuthorID.AUTHOR_ID;
import static org.sirix.xquery.function.sdb.trx.GetAuthorName.AUTHOR_NAME;
import static org.sirix.xquery.function.sdb.trx.GetChildCount.GET_CHILD_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetDescendantCount.GET_DESCENDANT_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetHash.HASH;
import static org.sirix.xquery.function.sdb.trx.GetMostRecentRevision.MOST_RECENT_REVISION;
import static org.sirix.xquery.function.sdb.trx.GetRevision.REVISION;
import static org.sirix.xquery.function.sdb.trx.GetRevisionTimestamp.TIMESTAMP;
import static org.sirix.xquery.function.sdb.trx.IsDeleted.IS_DELETED;
import static org.sirix.xquery.function.sdb.trx.ItemHistory.NODE_HISTORY;
import static org.sirix.xquery.function.sdb.trx.LevelOrder.LEVEL_ORDER;
import static org.sirix.xquery.function.sdb.trx.Rollback.ROLLBACK;
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
public final class SDBFun {
  /**
   * Prefix for Sirix functions.
   */
  public static final String SDB_PREFIX = "sdb";

  /**
   * Namespace URI for Sirix functions.
   */
  public static final String SDB_NSURI = "https://sirix.io";

  public static final QNm ERR_INVALID_ARGUMENT = new QNm(SDB_NSURI, SDB_PREFIX, "SIRIXDBF0001");

  public static final QNm ERR_INDEX_NOT_FOUND = new QNm(SDB_NSURI, SDB_PREFIX, "SIRIXDBF0002");

  public static final QNm ERR_FILE_NOT_FOUND = new QNm(SDB_NSURI, SDB_PREFIX, "SIRIXDBF0003");

  public static final QNm ERR_INVALID_INDEX_TYPE = new QNm(SDB_NSURI, SDB_PREFIX, "SIRIXDBF004");

  public static void register() {
    // dummy function to cause static block
    // to be executed exactly once
  }

  static {
    Namespaces.predefine(SDBFun.SDB_PREFIX, SDBFun.SDB_NSURI);

    // get path
    Functions.predefine(new GetPath(GetPath.GET_PATH, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // get nodeKey
    Functions.predefine(new GetNodeKey(GetNodeKey.GET_NODEKEY,
        new Signature(new SequenceType(AtomicType.INT, Cardinality.One), SequenceType.NODE)));

    // select item
    Functions.predefine(new SelectItem(SelectItem.SELECT_NODE,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(new AtomicType(Type.LON), Cardinality.One))));

    // select parent
    Functions.predefine(new SelectParent(SelectParent.SELECT_PARENT,
        new Signature(SequenceType.NODE, SequenceType.NODE)));

    // serialize
    Functions.predefine(new Serialize());

    // sort by document order
    Functions.predefine(
        new SortByDocOrder(SORT, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM_SEQUENCE)));

    // get number of descendants
    Functions.predefine(
        new GetDescendantCount(GET_DESCENDANT_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get number of descendants
    Functions.predefine(
        new GetDescendantCount(GET_DESCENDANT_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get number of children
    Functions.predefine(new GetChildCount(GET_CHILD_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get hash
    Functions.predefine(new GetHash(HASH, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // get timestamp
    Functions.predefine(new GetRevisionTimestamp(TIMESTAMP, new Signature(SequenceType.ITEM, SequenceType.NODE)));

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
    Functions.predefine(new Doc(DOC, new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
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

    // level-order
    Functions.predefine(new LevelOrder(LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.NODE,
        new SequenceType(AtomicType.INT, Cardinality.One))));
    Functions.predefine(new LevelOrder(LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.NODE)));

    // commit
    Functions.predefine(new Commit(COMMIT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // rollback
    Functions.predefine(new Rollback(ROLLBACK, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // revision
    Functions.predefine(new GetRevision(REVISION, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // most-recent-revision
    Functions.predefine(
        new GetMostRecentRevision(MOST_RECENT_REVISION, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-namespace-count
    Functions.predefine(
        new GetNamespaceCount(GET_NAMESPACE_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-attribute-count
    Functions.predefine(
        new GetNamespaceCount(GET_ATTRIBUTE_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // find-name-index
    Functions.predefine(new FindNameIndex(FIND_NAME_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.NODE, new SequenceType(AtomicType.QNM, Cardinality.One))));

    // find-path-index
    Functions.predefine(new FindPathIndex(FIND_PATH_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.NODE, SequenceType.STRING)));

    // find-cas-index
    Functions.predefine(new FindCASIndex(FIND_CAS_INDEX,
        new Signature(SequenceType.INTEGER, SequenceType.NODE, SequenceType.STRING, SequenceType.STRING)));

    // create-name-index
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.QNM, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // create-path-index
    Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // create-cas-index
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.STR, Cardinality.ZeroOrMany))));
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.ZeroOrOne))));
    Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // scan indexes
    Functions.predefine(new ScanPathIndex());
    Functions.predefine(new ScanCASIndex());
    Functions.predefine(new ScanCASIndexRange());
    Functions.predefine(new ScanNameIndex());

    // diff
    Functions.predefine(new Diff(DIFF,
        new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING, SequenceType.INTEGER,
            SequenceType.INTEGER)));

    // import
    Functions.predefine(new Import(IMPORT,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One))));

    // item-history
    Functions.predefine(new ItemHistory(NODE_HISTORY, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM)));

    // is-deleted
    Functions.predefine(new IsDeleted(IS_DELETED,
        new Signature(new SequenceType(AtomicType.BOOL, Cardinality.One), SequenceType.ITEM)));

    // author-name
    Functions.predefine(new GetAuthorName(AUTHOR_NAME, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // author-id
    Functions.predefine(new GetAuthorID(AUTHOR_ID, new Signature(SequenceType.STRING, SequenceType.NODE)));
  }
}
