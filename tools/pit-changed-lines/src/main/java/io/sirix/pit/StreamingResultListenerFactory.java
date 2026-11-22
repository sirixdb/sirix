package io.sirix.pit;

import java.util.Properties;
import org.pitest.mutationtest.ListenerArguments;
import org.pitest.mutationtest.MutationResultListener;
import org.pitest.mutationtest.MutationResultListenerFactory;

/**
 * Registers {@link StreamingResultListener} under the output format name {@code STREAM}.
 *
 * <pre>
 * outputFormats = ['HTML', 'XML', 'STREAM']
 * </pre>
 */
public final class StreamingResultListenerFactory implements MutationResultListenerFactory {

  @Override
  public String name() {
    return "STREAM";
  }

  @Override
  public String description() {
    return "Writes each analysed class's results as they arrive, so a killed run keeps them";
  }

  @Override
  public MutationResultListener getListener(final Properties properties,
      final ListenerArguments arguments) {
    return new StreamingResultListener(
        arguments.getOutputStrategy().createWriterForFile(StreamingResultListener.FILE));
  }
}
