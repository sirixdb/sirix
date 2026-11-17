package io.sirix.nativeimage.smoke;

import io.brackit.query.Query;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.jdm.Sequence;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import java.nio.file.Path;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

public final class QuerySmoke {
  @CEntryPoint(name = "smoke_query")
  public static int run(final IsolateThread thread, final CCharPointer path) {
    try {
      final Path location = Path.of(CTypeConversion.toJavaString(path));
      try (final BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location.getParent()).build();
           final SirixQueryContext context = SirixQueryContext.createWithJsonStore(store);
           final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
        // The harness supplies a fixed fixture name, never a client query fragment.
        final Sequence result = new Query(chain, "jn:doc('" + location.getFileName() + "','data').answer").execute(context);
        return ((IntNumeric) result).intValue();
      }
    } catch (final Throwable failure) {
      failure.printStackTrace();
      return -1;
    }
  }
}
