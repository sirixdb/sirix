package io.sirix.query.function.sdb;

import io.sirix.query.function.sdb.trx.Commit;
import io.sirix.query.function.sdb.trx.GetAuthorID;
import io.sirix.query.function.sdb.trx.GetAuthorName;
import io.sirix.query.function.sdb.trx.GetChildCount;
import io.sirix.query.function.sdb.trx.GetDescendantCount;
import io.sirix.query.function.sdb.trx.GetHash;
import io.sirix.query.function.sdb.trx.GetMostRecentRevision;
import io.sirix.query.function.sdb.trx.GetNodeKey;
import io.sirix.query.function.sdb.trx.GetPath;
import io.sirix.query.function.sdb.trx.GetRevision;
import io.sirix.query.function.sdb.trx.GetRevisionTimestamp;
import io.sirix.query.function.sdb.trx.GetValidFrom;
import io.sirix.query.function.sdb.trx.GetValidTo;
import io.sirix.query.function.sdb.trx.IsDeleted;
import io.sirix.query.function.sdb.trx.ItemHistory;
import io.sirix.query.function.sdb.trx.LevelOrder;
import io.sirix.query.function.sdb.trx.Rollback;
import io.sirix.query.function.sdb.trx.SelectItem;
import io.sirix.query.function.sdb.trx.SelectParent;
import io.sirix.query.function.xml.diff.Diff;
import io.sirix.query.function.xml.io.Serialize;
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
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.type.AtomicType;
import io.brackit.query.jdm.type.Cardinality;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.Functions;
import io.brackit.query.module.Namespaces;

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
    Functions.predefine(new SelectItem(SelectItem.SELECT_NODE, new Signature(SequenceType.NODE, SequenceType.NODE,
        new SequenceType(new AtomicType(Type.LON), Cardinality.One))));

    // select parent
    Functions.predefine(
        new SelectParent(SelectParent.SELECT_PARENT, new Signature(SequenceType.NODE, SequenceType.NODE)));

    // serialize
    Functions.predefine(new Serialize());

    // sort by document order
    Functions.predefine(
        new SortByDocOrder(SortByDocOrder.SORT, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM_SEQUENCE)));

    // get number of descendants
    Functions.predefine(new GetDescendantCount(GetDescendantCount.GET_DESCENDANT_COUNT,
        new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get number of descendants
    Functions.predefine(new GetDescendantCount(GetDescendantCount.GET_DESCENDANT_COUNT,
        new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get number of children
    Functions.predefine(
        new GetChildCount(GetChildCount.GET_CHILD_COUNT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get hash
    Functions.predefine(new GetHash(GetHash.HASH, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // get timestamp
    Functions.predefine(
        new GetRevisionTimestamp(GetRevisionTimestamp.TIMESTAMP, new Signature(SequenceType.ITEM, SequenceType.NODE)));

    // store
    Functions.predefine(new Store(false));
    Functions.predefine(new Store(true));
    Functions.predefine(new Store(Store.STORE, false));
    Functions.predefine(new Store(Store.STORE, true));

    // load
    Functions.predefine(new Load(false));
    Functions.predefine(new Load(true));
    Functions.predefine(new Load(Load.LOAD, false));
    Functions.predefine(new Load(Load.LOAD, true));

    // doc
    Functions.predefine(new Doc(Doc.DOC,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(Doc.DOC,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.INT, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));
    Functions.predefine(new Doc(Doc.DOC, new Signature(SequenceType.NODE,
        new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One))));

    // open
    Functions.predefine(new DocByPointInTime(DocByPointInTime.OPEN,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne))));
    Functions.predefine(new DocByPointInTime(DocByPointInTime.OPEN,
        new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne),
            new SequenceType(AtomicType.BOOL, Cardinality.ZeroOrOne))));

    // open-revisions
    Functions.predefine(new OpenRevisions(OpenRevisions.OPEN_REVISIONS,
        new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.STRING, SequenceType.STRING,
            new SequenceType(AtomicType.DATI, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.One))));

    // level-order
    Functions.predefine(new LevelOrder(LevelOrder.LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE,
        SequenceType.NODE, new SequenceType(AtomicType.INT, Cardinality.One))));
    Functions.predefine(
        new LevelOrder(LevelOrder.LEVEL_ORDER, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.NODE)));

    // commit
    Functions.predefine(new Commit(Commit.COMMIT, new Signature(SequenceType.INTEGER, SequenceType.NODE)));
    Functions.predefine(new Commit(Commit.COMMIT,
        new Signature(SequenceType.INTEGER, SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One))));
    Functions.predefine(new Commit(Commit.COMMIT, new Signature(SequenceType.INTEGER, SequenceType.NODE,
        new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.DATI, Cardinality.One))));

    // rollback
    Functions.predefine(new Rollback(Rollback.ROLLBACK, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // revision
    Functions.predefine(new GetRevision(GetRevision.REVISION, new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // most-recent-revision
    Functions.predefine(new GetMostRecentRevision(GetMostRecentRevision.MOST_RECENT_REVISION,
        new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-namespace-count
    Functions.predefine(new GetNamespaceCount(GetNamespaceCount.GET_NAMESPACE_COUNT,
        new Signature(SequenceType.INTEGER, SequenceType.NODE)));

    // get-attribute-count
    Functions.predefine(new GetNamespaceCount(GetAttributeCount.GET_ATTRIBUTE_COUNT,
        new Signature(SequenceType.INTEGER, SequenceType.NODE)));

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
    Functions.predefine(new Diff(Diff.DIFF, new Signature(SequenceType.STRING, SequenceType.STRING, SequenceType.STRING,
        SequenceType.INTEGER, SequenceType.INTEGER)));

    // import
    Functions.predefine(
        new Import(Import.IMPORT, new Signature(SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
            new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.STR, Cardinality.One))));

    // item-history
    Functions.predefine(
        new ItemHistory(ItemHistory.NODE_HISTORY, new Signature(SequenceType.ITEM_SEQUENCE, SequenceType.ITEM)));

    // is-deleted
    Functions.predefine(new IsDeleted(IsDeleted.IS_DELETED,
        new Signature(new SequenceType(AtomicType.BOOL, Cardinality.One), SequenceType.ITEM)));

    // author-name
    Functions.predefine(
        new GetAuthorName(GetAuthorName.AUTHOR_NAME, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // author-id
    Functions.predefine(new GetAuthorID(GetAuthorID.AUTHOR_ID, new Signature(SequenceType.STRING, SequenceType.NODE)));

    // valid-from (bitemporal)
    Functions.predefine(new GetValidFrom(GetValidFrom.VALID_FROM,
        new Signature(new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne), SequenceType.ITEM)));

    // valid-to (bitemporal)
    Functions.predefine(new GetValidTo(GetValidTo.VALID_TO,
        new Signature(new SequenceType(AtomicType.DATI, Cardinality.ZeroOrOne), SequenceType.ITEM)));
  }
}
