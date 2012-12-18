package org.sirix.xquery.function.sdb;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.module.Namespaces;

public class SDBFun {
	public static final String SIRIXDB_PREFIX = "sirix";

	public static final String SIRIXDB_NSURI = "https://github.com/JohannesLichtenberger/sirix";

	public static final QNm ERR_INVALID_ARGUMENT = new QNm(SIRIXDB_NSURI,
			SIRIXDB_PREFIX, "SIRIXDBF0001");

	public static final QNm ERR_INDEX_NOT_FOUND = null;

	public static final QNm ERR_INVALID_INDEX_TYPE = null;

	public static void register() {
		// dummy function to cause static block
		// to be executed exactly once
	}

	static {
		Namespaces.predefine(SDBFun.SIRIXDB_PREFIX, SDBFun.SIRIXDB_NSURI);
	}
}
