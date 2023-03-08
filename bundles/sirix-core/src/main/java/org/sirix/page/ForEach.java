package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.PageContainer;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

public class ForEach extends RecursiveAction {
  private final static int TASK_LEN = 1024 / 8;
  private final TransactionIntentLog log;
  private final PageReadOnlyTrx pageTrx;
  private final PageReference[] array;
  private final int from;
  private final int to;

  public ForEach(TransactionIntentLog log, PageReadOnlyTrx pageTrx, PageReference[] array, int from, int to) {
    this.log = log;
    this.pageTrx = pageTrx;
    this.array = array;
    this.from = from;
    this.to = to;
  }

  @Override
  protected void compute() {
    int len = to - from;
    if (len < TASK_LEN) {
      work(log, pageTrx, array, from, to);
    } else {
      // split work in half, execute sub-tasks asynchronously
      int mid = (from + to) >>> 1;
      final List<ForEach> dividedTasks = new ArrayList<>();
      dividedTasks.add(new ForEach(log, pageTrx, array, from, mid));
      dividedTasks.add(new ForEach(log, pageTrx, array, mid, to));
      ForkJoinTask.invokeAll(dividedTasks).forEach(ForkJoinTask::join);
    }
  }

  private void work(TransactionIntentLog log, PageReadOnlyTrx pageTrx, PageReference[] references, int from, int to) {
    for (int j = from; j < to; j++) {
      final var reference = references[j];
      if (reference != null && (reference.getLogKey() != Constants.NULL_ID_INT)) {
        final PageContainer container = log.get(reference);
        final Page page = container.getModified();
      }
    }
  }
}
