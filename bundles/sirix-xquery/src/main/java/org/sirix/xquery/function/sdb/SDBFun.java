package org.sirix.xquery.function.sdb;

import static org.sirix.xquery.function.sdb.index.FindCASIndex.FIND_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.FindNameIndex.FIND_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.FindPathIndex.FIND_PATH_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreateNameIndex.CREATE_NAME_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreatePathIndex.CREATE_PATH_INDEX;
import static org.sirix.xquery.function.sdb.io.Doc.DOC;
import static org.sirix.xquery.function.sdb.io.Load.LOAD;
import static org.sirix.xquery.function.sdb.io.Store.STORE;
import static org.sirix.xquery.function.sdb.trx.Commit.COMMIT;
import static org.sirix.xquery.function.sdb.trx.GetMostRecentRevision.MOST_RECENT_REVISION;
import static org.sirix.xquery.function.sdb.trx.Rollback.ROLLBACK;
import static org.sirix.xquery.function.sdb.datamining.GetDescendantCount.DESCENDANTS;
import static org.sirix.xquery.function.sdb.datamining.GetChildCount.CHILDREN;
import static org.sirix.xquery.function.sdb.datamining.GetHash.HASH;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.sdb.datamining.GetChildCount;
import org.sirix.xquery.function.sdb.datamining.GetDescendantCount;
import org.sirix.xquery.function.sdb.datamining.GetHash;
import org.sirix.xquery.function.sdb.index.FindCASIndex;
import org.sirix.xquery.function.sdb.index.FindNameIndex;
import org.sirix.xquery.function.sdb.index.FindPathIndex;
import org.sirix.xquery.function.sdb.index.create.CreateCASIndex;
import org.sirix.xquery.function.sdb.index.create.CreateNameIndex;
import org.sirix.xquery.function.sdb.index.create.CreatePathIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanCASIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanNameIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanPathIndex;
import org.sirix.xquery.function.sdb.io.Doc;
import org.sirix.xquery.function.sdb.io.Load;
import org.sirix.xquery.function.sdb.io.Store;
import org.sirix.xquery.function.sdb.trx.Commit;
import org.sirix.xquery.function.sdb.trx.GetMostRecentRevision;
import org.sirix.xquery.function.sdb.trx.Rollback;

public final class SDBFun {
	public static final String SDB_PREFIX = "sdb";

	public static final String SDB_NSURI = "https://github.com/sirixdb/sirix";

	public static final QNm ERR_INVALID_ARGUMENT = new QNm(SDB_NSURI, SDB_PREFIX,
			"SIRIXDBF0001");

	public static final QNm ERR_INDEX_NOT_FOUND = new QNm(SDB_NSURI, SDB_PREFIX,
			"SIRIXDBF0002");

	public static final QNm ERR_INVALID_INDEX_TYPE = null;

	public static void register() {
		// dummy function to cause static block
		// to be executed exactly once
	}

	static {
		Namespaces.predefine(SDBFun.SDB_PREFIX, SDBFun.SDB_NSURI);

		// get number of descendants
		Functions.predefine(new GetDescendantCount(DESCENDANTS, new Signature(SequenceType.INTEGER,
				SequenceType.NODE)));
		
		// get number of children
		Functions.predefine(new GetChildCount(CHILDREN, new Signature(SequenceType.INTEGER,
				SequenceType.NODE)));
		
		// get hash
		Functions.predefine(new GetHash(HASH, new Signature(SequenceType.STRING,
				SequenceType.NODE)));
		
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
		Functions.predefine(new Doc(DOC, new Signature(SequenceType.NODE,
				new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(
						AtomicType.STR, Cardinality.One), new SequenceType(AtomicType.INT,
						Cardinality.ZeroOrOne))));
		Functions.predefine(new Doc(DOC, new Signature(SequenceType.NODE,
				new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(
						AtomicType.STR, Cardinality.One))));

		// commit
		Functions.predefine(new Commit(COMMIT, new Signature(SequenceType.INTEGER,
				SequenceType.NODE)));
		
		// rollback
		Functions.predefine(new Rollback(ROLLBACK, new Signature(SequenceType.INTEGER,
				SequenceType.NODE)));

		// get-most-recent-rev
		Functions.predefine(new GetMostRecentRevision(MOST_RECENT_REVISION,
				new Signature(SequenceType.INTEGER, SequenceType.NODE)));
		
		// find-name-index
		Functions.predefine(new FindNameIndex(FIND_NAME_INDEX, new Signature(
				SequenceType.INTEGER, SequenceType.NODE,  new SequenceType(
						AtomicType.QNM, Cardinality.One))));
		
		// find-path-index
		Functions.predefine(new FindPathIndex(FIND_PATH_INDEX, new Signature(
				SequenceType.INTEGER, SequenceType.NODE, SequenceType.STRING)));

		// find-cas-index
		Functions.predefine(new FindCASIndex(FIND_CAS_INDEX, new Signature(
				SequenceType.INTEGER, SequenceType.NODE, SequenceType.STRING)));

		// create-name-index
		Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.QNM,
						Cardinality.ZeroOrMany))));
		Functions.predefine(new CreateNameIndex(CREATE_NAME_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE)));
		
		// create-path-index
		Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR,
						Cardinality.ZeroOrMany))));
		Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE)));

		// create-cas-index
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR,
						Cardinality.ZeroOrOne), new SequenceType(AtomicType.STR,
						Cardinality.ZeroOrMany))));
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR,
						Cardinality.ZeroOrOne))));
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, SequenceType.NODE)));

		// scan indexes
		Functions.predefine(new ScanPathIndex());
		Functions.predefine(new ScanCASIndex());
		Functions.predefine(new ScanNameIndex());
	}
}
