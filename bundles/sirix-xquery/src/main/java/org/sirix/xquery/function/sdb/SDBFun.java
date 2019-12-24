package org.sirix.xquery.function.sdb;

import static org.sirix.xquery.function.sdb.diff.Diff.DIFF;
import static org.sirix.xquery.function.sdb.index.SortByDocOrder.SORT;
import static org.sirix.xquery.function.sdb.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindCASIndex.FIND_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindNameIndex.FIND_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.find.FindPathIndex.FIND_PATH_INDEX;
import static org.sirix.xquery.function.sdb.io.Doc.DOC;
import static org.sirix.xquery.function.sdb.io.DocByPointInTime.OPEN;
import static org.sirix.xquery.function.sdb.io.Import.IMPORT;
import static org.sirix.xquery.function.sdb.io.Load.LOAD;
import static org.sirix.xquery.function.sdb.io.OpenRevisions.OPEN_REVISIONS;
import static org.sirix.xquery.function.sdb.io.Store.STORE;
import static org.sirix.xquery.function.sdb.trx.Commit.COMMIT;
import static org.sirix.xquery.function.sdb.trx.GetAttributeCount.GET_ATTRIBUTE_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetChildCount.GET_CHILD_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetDescendantCount.GET_DESCENDANT_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetHash.HASH;
import static org.sirix.xquery.function.sdb.trx.GetMostRecentRevision.MOST_RECENT_REVISION;
import static org.sirix.xquery.function.sdb.trx.GetNamespaceCount.GET_NAMESPACE_COUNT;
import static org.sirix.xquery.function.sdb.trx.GetRevision.REVISION;
import static org.sirix.xquery.function.sdb.trx.GetRevisionTimestamp.TIMESTAMP;
import static org.sirix.xquery.function.sdb.trx.LevelOrder.LEVEL_ORDER;
import static org.sirix.xquery.function.sdb.trx.Rollback.ROLLBACK;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.sdb.diff.Diff;
import org.sirix.xquery.function.sdb.index.SortByDocOrder;
import org.sirix.xquery.function.sdb.index.create.CreateCASIndex;
import org.sirix.xquery.function.sdb.index.create.CreateNameIndex;
import org.sirix.xquery.function.sdb.index.create.CreatePathIndex;
import org.sirix.xquery.function.sdb.index.find.FindCASIndex;
import org.sirix.xquery.function.sdb.index.find.FindNameIndex;
import org.sirix.xquery.function.sdb.index.find.FindPathIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanCASIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanCASIndexRange;
import org.sirix.xquery.function.sdb.index.scan.ScanNameIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanPathIndex;
import org.sirix.xquery.function.sdb.io.Doc;
import org.sirix.xquery.function.sdb.io.DocByPointInTime;
import org.sirix.xquery.function.sdb.io.Import;
import org.sirix.xquery.function.sdb.io.Load;
import org.sirix.xquery.function.sdb.io.OpenRevisions;
import org.sirix.xquery.function.sdb.io.Serialize;
import org.sirix.xquery.function.sdb.io.Store;
import org.sirix.xquery.function.sdb.trx.Commit;
import org.sirix.xquery.function.sdb.trx.GetChildCount;
import org.sirix.xquery.function.sdb.trx.GetDescendantCount;
import org.sirix.xquery.function.sdb.trx.GetHash;
import org.sirix.xquery.function.sdb.trx.GetMostRecentRevision;
import org.sirix.xquery.function.sdb.trx.GetNamespaceCount;
import org.sirix.xquery.function.sdb.trx.GetNodeKey;
import org.sirix.xquery.function.sdb.trx.GetPath;
import org.sirix.xquery.function.sdb.trx.GetRevision;
import org.sirix.xquery.function.sdb.trx.GetRevisionTimestamp;
import org.sirix.xquery.function.sdb.trx.LevelOrder;
import org.sirix.xquery.function.sdb.trx.Rollback;
import org.sirix.xquery.function.sdb.trx.SelectNode;

/**
 * Function definitions.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SDBFun {
  /** Prefix for Sirix functions. */
  public static final String SDB_PREFIX = "sdb";

  /** Namespace URI for Sirix functions. */
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

    // move to
    Functions.predefine(new SelectNode(SelectNode.SELECT_NODE,
        new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.INT, Cardinality.One))));

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
    Functions.predefine(new Diff(DIFF, new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING,
        SequenceType.INTEGER, SequenceType.INTEGER)));

    // import
    Functions.predefine(
        new Import(IMPORT, new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One))));
  }
}
