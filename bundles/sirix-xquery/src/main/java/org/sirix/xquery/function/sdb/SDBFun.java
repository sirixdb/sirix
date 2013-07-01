package org.sirix.xquery.function.sdb;

import static org.sirix.xquery.function.sdb.index.create.CreateCASIndex.CREATE_CAS_INDEX;
import static org.sirix.xquery.function.sdb.index.create.CreateCASIndexFromDoc.CREATE_CAS_INDEX_FROM_DOC;
import static org.sirix.xquery.function.sdb.index.create.CreatePathIndex.CREATE_PATH_INDEX;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Functions;
import org.brackit.xquery.module.Namespaces;
import org.brackit.xquery.xdm.Signature;
import org.brackit.xquery.xdm.type.AtomicType;
import org.brackit.xquery.xdm.type.Cardinality;
import org.brackit.xquery.xdm.type.SequenceType;
import org.sirix.xquery.function.sdb.index.create.CreateCASIndex;
import org.sirix.xquery.function.sdb.index.create.CreateCASIndexFromDoc;
import org.sirix.xquery.function.sdb.index.create.CreatePathIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanCASIndex;
import org.sirix.xquery.function.sdb.index.scan.ScanPathIndex;

public final class SDBFun {
	public static final String SDB_PREFIX = "sdb";

	public static final String SDB_NSURI = "https://github.com/sirixdb/sirix";

	public static final QNm ERR_INVALID_ARGUMENT = new QNm(SDB_NSURI, SDB_PREFIX,
			"SIRIXDBF0001");

	public static final QNm ERR_INDEX_NOT_FOUND = null;

	public static final QNm ERR_INVALID_INDEX_TYPE = null;

	public static void register() {
		// dummy function to cause static block
		// to be executed exactly once
	}

	static {
		Namespaces.predefine(SDBFun.SDB_PREFIX, SDBFun.SDB_NSURI);

		Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(
				SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrMany))));
		Functions.predefine(new CreatePathIndex(CREATE_PATH_INDEX, new Signature(
				SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AtomicType.STR, Cardinality.One))));
		Functions.predefine(new CreateCASIndexFromDoc(CREATE_CAS_INDEX_FROM_DOC,
				new Signature(SequenceType.NODE, SequenceType.NODE, new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrOne), new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrMany))));
		Functions.predefine(new CreateCASIndexFromDoc(CREATE_CAS_INDEX_FROM_DOC, new Signature(
				SequenceType.NODE, SequenceType.NODE, new SequenceType(AtomicType.STR,
						Cardinality.ZeroOrOne))));
		Functions.predefine(new CreateCASIndexFromDoc(CREATE_CAS_INDEX_FROM_DOC, new Signature(
				SequenceType.NODE, SequenceType.NODE)));
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrOne), new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrMany))));
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AtomicType.STR, Cardinality.One), new SequenceType(
						AtomicType.STR, Cardinality.ZeroOrOne))));
		Functions.predefine(new CreateCASIndex(CREATE_CAS_INDEX, new Signature(
				SequenceType.NODE, new SequenceType(AtomicType.STR, Cardinality.One),
				new SequenceType(AtomicType.STR, Cardinality.One))));

		Functions.predefine(new ScanPathIndex());
		Functions.predefine(new ScanCASIndex());
	}
}
