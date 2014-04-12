package org.sirix.page;

public final class PageConstants {

	private PageConstants() {
		throw new AssertionError("May never be instantiated!");
	}

	// 150 KiB.
	public static final int MAX_RECORD_SIZE = 150_000;

	public static final int MAX_INDEX_NR = 512;
}
