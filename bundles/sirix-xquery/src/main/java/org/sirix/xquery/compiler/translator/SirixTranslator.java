package org.sirix.xquery.compiler.translator;

import java.util.Map;

import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.compiler.translator.TopDownTranslator;
import org.brackit.xquery.util.Cfg;

/**
 * @author Sebastian Baechle
 * 
 */
public class SirixTranslator extends TopDownTranslator {

	public static final boolean OPTIMIZE = Cfg.asBool(
			"org.brackit.server.xquery.optimize.accessor", false);

	public SirixTranslator(Map<QNm, Str> options) {
		super(options);
	}
}
