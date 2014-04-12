/**
 * Copyright (c) 2010, Distributed Systems Group, University of Konstanz
 * 
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 * 
 * THE SOFTWARE IS PROVIDED AS IS AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 * 
 */
package org.sirix.wikipedia.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.WritableComparable;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * <h1>DateWritable</h1>
 * 
 * <p>
 * Simple date wrapper which implements {@link WritableComparable}, so it can be
 * used as a key.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class DateWritable implements WritableComparable<DateWritable> {

	/**
	 * {@link LogWrapper} used for logging.
	 */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(DateWritable.class));

	/** {@link DateFormat}. */
	private final DateFormat mFormatter = new SimpleDateFormat(
			"yyyy.MM.dd HH.mm.ss", Locale.ENGLISH);

	/** {@link Date} used as the key. */
	private Date mTimestamp;

	/**
	 * Constructor.
	 */
	public DateWritable() {
	}

	/**
	 * Constructor.
	 * 
	 * @param paramTimestamp
	 *          The {@link Date} to use as the key.
	 */
	public DateWritable(final Date paramTimestamp) {
		mTimestamp = paramTimestamp;
	}

	/**
	 * Set timestamp.
	 * 
	 * @param paramTimestamp
	 *          The Timestamp to set.
	 */
	public void setTimestamp(final Date paramTimestamp) {
		mTimestamp = paramTimestamp;
	}

	@Override
	public void readFields(final DataInput paramIn) throws IOException {
		try {
			mTimestamp = mFormatter.parse(paramIn.readUTF());
		} catch (final ParseException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	public void write(final DataOutput paramOut) throws IOException {
		paramOut.writeUTF(mFormatter.format(mTimestamp));
	}

	@Override
	public int compareTo(final DateWritable paramDate) {
		int retVal = 0;

		if (paramDate.mTimestamp.before(this.mTimestamp)) {
			retVal = 1;
		} else if (paramDate.mTimestamp.after(this.mTimestamp)) {
			retVal = -1;
		}

		return retVal;
	}
}
