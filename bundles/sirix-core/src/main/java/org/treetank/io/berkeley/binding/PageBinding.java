package org.treetank.io.berkeley.binding;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBase;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

import java.io.IOException;

import org.slf4j.LoggerFactory;
import org.treetank.io.berkeley.TupleInputSink;
import org.treetank.io.berkeley.TupleOutputSink;
import org.treetank.page.PagePersistenter;
import org.treetank.page.interfaces.IPage;
import org.treetank.utils.LogWrapper;
import org.xerial.snappy.Snappy;

/**
 * Compresses pages with Snappy.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class PageBinding extends TupleBase<IPage> implements EntryBinding<IPage> {

  /** {@link LogWrapper} instance. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory.getLogger(PageBinding.class));

  @Override
  public IPage entryToObject(final DatabaseEntry pEntry) {
    TupleInput tupleInput = null;
    try {
      tupleInput = new TupleInput(Snappy.uncompress(pEntry.getData()));
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    return PagePersistenter.deserializePage(new TupleInputSink(tupleInput));
  }

  @Override
  public void objectToEntry(final IPage pPage, final DatabaseEntry pEntry) {
    try {
      TupleOutput output = getTupleOutput(pPage);
      PagePersistenter.serializePage(new TupleOutputSink(output), pPage);
      pEntry.setData(Snappy.compress(output.getBufferBytes()));
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
  }

}
