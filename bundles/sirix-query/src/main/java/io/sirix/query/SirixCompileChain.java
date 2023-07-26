package io.sirix.query;

import java.util.Map;

import io.sirix.query.compiler.optimizer.SirixOptimizer;
import io.sirix.query.compiler.translator.SirixTranslator;
import io.sirix.query.function.jn.JNFun;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.node.XmlDBStore;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.compiler.optimizer.Optimizer;
import org.brackit.xquery.compiler.translator.Translator;
import org.brackit.xquery.util.Cfg;

/**
 * Compile chain.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixCompileChain extends CompileChain implements AutoCloseable {
  public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.indexrewrite", true);

  static {
    // define function namespaces and functions in these namespaces
    SDBFun.register();
    XMLFun.register();
    JNFun.register();
  }

  /** The XML node store. */
  private final XmlDBStore nodeStore;

  /** The JSON item store. */
  private final JsonDBStore jsonItemStore;

  public static SirixCompileChain create() {
    return new SirixCompileChain(null, null);
  }

  public static SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixCompileChain(nodeStore, null);
  }

  public static SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore);
  }

  public static SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore,
      final JsonDBStore jsonStore) {
    return new SirixCompileChain(nodeStore, jsonStore);
  }

  /**
   * Constructor.
   *
   * @param nodeStore the Sirix {@link BasicXmlDBStore}
   * @param jsonItemStore the json item store.
   */
  public SirixCompileChain(final XmlDBStore nodeStore, final JsonDBStore jsonItemStore) {
    this.nodeStore = nodeStore == null
        ? BasicXmlDBStore.newBuilder().build()
        : nodeStore;
    this.jsonItemStore = jsonItemStore == null
        ? BasicJsonDBStore.newBuilder().build()
        : jsonItemStore;
  }

  @Override
  protected Translator getTranslator(Map<QNm, Str> options) {
    return new SirixTranslator(options);
  }

  @Override
  protected Optimizer getOptimizer(Map<QNm, Str> options) {
    if (!OPTIMIZE) {
      return super.getOptimizer(options);
    }
    return new SirixOptimizer(options, nodeStore, jsonItemStore);
  }

  @Override
  public void close() {
    nodeStore.close();
    jsonItemStore.close();
  }
}
