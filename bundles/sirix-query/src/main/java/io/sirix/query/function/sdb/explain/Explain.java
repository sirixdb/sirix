package io.sirix.query.function.sdb.explain;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Bool;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.module.StaticContext;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.node.XmlDBStore;

/**
 * The {@code sdb:explain} function — compiles a query through the full optimizer
 * pipeline and returns the query plan as a JSON string.
 *
 * <p>Supported signatures:
 * <ul>
 *   <li>{@code sdb:explain($query as xs:string) as xs:string}</li>
 *   <li>{@code sdb:explain($query as xs:string, $verbose as xs:boolean?) as xs:string}</li>
 * </ul>
 *
 * <p>The function compiles the inner query through all 10 optimizer stages
 * (JQGM rewrites, cost-based analysis, join reordering, mesh selection,
 * index decomposition, cost-driven routing, vectorized detection/routing,
 * and index matching) but does <b>not execute it</b>.</p>
 *
 * <p>When {@code $verbose} is true, the output includes both the parsed AST
 * (before optimization) and the optimized AST (after optimization).</p>
 *
 * <p>Example:
 * <pre>{@code
 * sdb:explain('for $x in jn:doc("db","res")[][] where $x.price > 50 return $x')
 * }</pre>
 */
public final class Explain extends AbstractFunction {

  /** Function name: sdb:explain */
  public static final QNm EXPLAIN = new QNm(SDBFun.SDB_NSURI, SDBFun.SDB_PREFIX, "explain");

  /**
   * Constructor.
   *
   * @param name      the function name
   * @param signature the function signature
   */
  public Explain(final QNm name, final Signature signature) {
    super(name, signature, true);
  }

  @Override
  public Sequence execute(final StaticContext sctx, final QueryContext ctx, final Sequence[] args)
      throws QueryException {
    final String queryString = ((Str) args[0]).stringValue();

    // Second argument: true for verbose, or "candidates" string for candidate plans
    final boolean verbose;
    final boolean showCandidates;
    if (args.length >= 2 && args[1] != null) {
      final Item arg1 = (Item) args[1];
      if (arg1 instanceof Str str) {
        showCandidates = "candidates".equalsIgnoreCase(str.stringValue());
        verbose = !showCandidates;
      } else {
        showCandidates = false;
        verbose = ((Bool) arg1).booleanValue();
      }
    } else {
      verbose = false;
      showCandidates = false;
    }

    // Borrow stores from the current query context
    final JsonDBStore jsonStore;
    final XmlDBStore xmlStore;
    if (ctx instanceof SirixQueryContext sqc) {
      jsonStore = sqc.getJsonItemStore();
      xmlStore = sqc.getNodeStore();
    } else {
      throw new QueryException(
          new QNm("sdb:explain requires a SirixQueryContext"));
    }

    // Compile the inner query through the full optimizer pipeline.
    // We intentionally do NOT close this chain — close() would close the
    // borrowed stores that belong to the outer query context.
    final var chain = new SirixCompileChain(xmlStore, jsonStore);
    try {
      chain.compile(queryString);
    } catch (final Exception e) {
      throw new QueryException(
          new QNm("Failed to compile query for EXPLAIN: " + e.getMessage()), e);
    }

    final String json;
    if (showCandidates) {
      json = QueryPlanSerializer.serializeWithCandidates(
          chain.getOptimizedAST(), chain.getMesh());
    } else if (verbose) {
      json = QueryPlanSerializer.serializeBoth(
          chain.getParsedAST(), chain.getOptimizedAST());
    } else {
      json = QueryPlanSerializer.serialize(chain.getOptimizedAST());
    }

    return new Str(json);
  }
}
