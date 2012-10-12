package org.sirix.xquery;

import java.util.Map;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.CompileChain;
import org.brackit.xquery.compiler.optimizer.Optimizer;
import org.brackit.xquery.compiler.translator.Translator;
import org.sirix.xquery.compiler.optimizer.DBOptimizer;

public class DBCompileChain extends CompileChain {
//
//
//	@Override
//	protected Translator getTranslator(Map<QNm, Str> options) {
//		return new DBTranslator(options);
//	}
//
//	@Override
//	protected Optimizer getOptimizer(Map<QNm, Str> options) {
//		if (!OPTIMIZE) {
//			return super.getOptimizer(options);
//		}
//		return new DBOptimizer(options, mdm, tx);
//	}
}
