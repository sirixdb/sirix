package io.sirix.access;

public final class AggregateKernelTestSupport {
  private AggregateKernelTestSupport() {}

  public static ResourceConfiguration.Builder newBuilder(final String resourceName) {
    return ResourceConfiguration.newBuilder(resourceName).aggregateKernel(true);
  }
}
