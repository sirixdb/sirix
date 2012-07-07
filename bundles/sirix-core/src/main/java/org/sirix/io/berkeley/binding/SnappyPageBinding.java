package org.sirix.io.berkeley.binding;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

import java.io.IOException;

import javax.annotation.Nullable;

import org.sirix.io.berkeley.TupleInputSink;
import org.sirix.io.berkeley.TupleOutputSink;
import org.sirix.page.PagePersistenter;
import org.sirix.page.interfaces.IPage;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

/**
 * Compresses pages with Snappy.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class SnappyPageBinding extends TupleBase<IPage> implements
  EntryBinding<IPage> {

  /** {@link LogWrapper} instance. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(SnappyPageBinding.class));

  @Override
  public IPage entryToObject(@Nullable final DatabaseEntry pEntry) {
    if (pEntry == null) {
      return null;
    }
    TupleInput tupleInput = null;
    try {
      tupleInput = new TupleInput(Snappy.uncompress(pEntry.getData()));
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    if (tupleInput != null) {
      return PagePersistenter.deserializePage(new TupleInputSink(tupleInput));
    } else {
      return null;
    }
  }

  @Override
  public void objectToEntry(@Nullable final IPage pPage,
    @Nullable final DatabaseEntry pEntry) {
    if (pPage != null && pEntry != null) {
      try {
        TupleOutput output = getTupleOutput(pPage);
        PagePersistenter.serializePage(new TupleOutputSink(output), pPage);
        pEntry.setData(Snappy.compress(output.getBufferBytes()));
      } catch (final IOException e) {
        LOGWRAPPER.error(e.getMessage(), e);
      }
    }
  }

}
