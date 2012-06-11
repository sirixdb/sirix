package org.treetank.gui;

import javax.swing.JOptionPane;

final class ExceptionReporting implements Thread.UncaughtExceptionHandler {
  private ExceptionReporting() {
  }

  private static ExceptionReporting exceptionReporter;

  /**
   * Sets up a default handler for uncaught exceptions
   */
  public static synchronized void registerExceptionReporter() {
    if (exceptionReporter == null) {
      exceptionReporter = new ExceptionReporting();
    }

    Thread.setDefaultUncaughtExceptionHandler(exceptionReporter);

    ThreadGroup root = Thread.currentThread().getThreadGroup().getParent();
    while (root.getParent() != null) {
      root = root.getParent();
    }

    // Visit each thread group
    visit(root, 0, exceptionReporter);
  }

  // This method recursively visits all thread groups under `group'.
  public static void visit(ThreadGroup group, int level, ExceptionReporting exceptionReporter) {
    // Get threads in `group'
    int numThreads = group.activeCount();
    Thread[] threads = new Thread[numThreads * 2];
    numThreads = group.enumerate(threads, false);

    // Enumerate each thread in `group'
    for (int i = 0; i < numThreads; i++) { // Get thread
      Thread thread = threads[i];
      thread.setUncaughtExceptionHandler(exceptionReporter);
    }

    // Get thread subgroups of `group'
    int numGroups = group.activeGroupCount();
    ThreadGroup[] groups = new ThreadGroup[numGroups * 2];
    numGroups = group.enumerate(groups, false);

    // Recursively visit each subgroup
    for (int i = 0; i < numGroups; i++) {
      visit(groups[i], level + 1, exceptionReporter);
    }
  }

  /**
   * Catches exceptions and notifies the server service
   */
  @Override
  public void uncaughtException(final Thread t, final Throwable e) {
    JOptionPane.showMessageDialog(null, e.fillInStackTrace() + " Error message: " + e.getMessage(),
      "Exception occurred", JOptionPane.ERROR_MESSAGE);
  }
}
