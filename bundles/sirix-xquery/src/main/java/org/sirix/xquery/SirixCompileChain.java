package org.sirix.xquery;

import java.util.Map;

import javax.annotation.Nonnull;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.compiler.optimizer.Optimizer;
import org.brackit.xquery.compiler.translator.Translator;
import org.brackit.xquery.util.Cfg;
import org.sirix.xquery.compiler.optimizer.SirixOptimizer;
import org.sirix.xquery.compiler.translator.SirixTranslator;
import org.sirix.xquery.function.sdb.SDBFun;
import org.sirix.xquery.node.DBStore;

/**
 * Compile chain.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public final class SirixCompileChain extends CompileChain {
	public static final boolean OPTIMIZE = Cfg.asBool(
			"org.sirix.xquery.optimize.multichild", false);

	static {
		// define function namespaces and functions in these namespaces
		SDBFun.register();
	}

	/** The Sirix {@link DBStore}. */
	private final DBStore mStore;

	/**
	 * Constructor.
	 * 
	 * @param store
	 *          the Sirix {@link DBStore}
	 */
	public SirixCompileChain(final DBStore store) {
		mStore = store;
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
		return new SirixOptimizer(options, mStore);
	}
}
