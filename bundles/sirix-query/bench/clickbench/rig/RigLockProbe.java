package io.sirix.query.bench.clickbench;

import java.nio.file.Path;

/** Small subprocess for kernel-lease integration checks; never opens a database. */
public final class RigLockProbe {
  public static void main(final String[] args) throws Exception {
    if (args.length != 4) {
      throw new IllegalArgumentException("expected path, exclusive, inherited descriptor or none, close or hold");
    }
    final String inherited = "none".equals(args[2]) ? null : args[2];
    try (ClickBenchRigLease lease = ClickBenchRigLease.acquire(Path.of(args[0]),
        Boolean.parseBoolean(args[1]), inherited)) {
      if ("close".equals(args[3])) {
        lease.close();
      }
      System.out.println("READY");
      System.out.flush();
      // Remains alive independently of the parent's stdin or process lifetime.
      Thread.sleep(60_000);
    }
  }
}
