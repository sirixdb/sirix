package org.sirix.xquery;

import java.util.Map;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.compiler.optimizer.Optimizer;
import org.brackit.xquery.compiler.translator.Translator;
import org.brackit.xquery.util.Cfg;
import org.sirix.xquery.compiler.optimizer.SirixOptimizer;
import org.sirix.xquery.compiler.translator.SirixTranslator;
import org.sirix.xquery.function.jn.JNFun;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.function.xml.XMLFun;
import org.sirix.xquery.json.BasicJsonDBStore;
import org.sirix.xquery.json.JsonDBStore;
import org.sirix.xquery.node.BasicXmlDBStore;
import org.sirix.xquery.node.XmlDBStore;

/**
 * Compile chain.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixCompileChain extends CompileChain implements AutoCloseable {
  public static final boolean OPTIMIZE = Cfg.asBool("org.sirix.xquery.optimize.indexrewrite", false);

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

  public static final SirixCompileChain create() {
    return new SirixCompileChain(null, null);
  }

  public static final SirixCompileChain createWithNodeStore(final XmlDBStore nodeStore) {
    return new SirixCompileChain(nodeStore, null);
  }

  public static final SirixCompileChain createWithJsonStore(final JsonDBStore jsonStore) {
    return new SirixCompileChain(null, jsonStore);
  }

  public static final SirixCompileChain createWithNodeAndJsonStore(final XmlDBStore nodeStore,
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
