package org.sirix.gui.view.model;

public class TextLength {
	private final int mMin;
	private final int mMax;

	public TextLength(final int paramMin, final int paramMax) {
		mMin = paramMin;
		mMax = paramMax;
	}

	public int getMax() {
		return mMax;
	}

	public int getMin() {
		return mMin;
	}
}
