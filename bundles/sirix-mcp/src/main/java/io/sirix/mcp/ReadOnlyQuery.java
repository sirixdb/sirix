package io.sirix.mcp;

import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.BaseResolver;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.analyzer.Analyzer;
import io.brackit.query.compiler.parser.JsoniqParser;
import io.brackit.query.function.io.IOFun;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.module.Namespaces;
import io.brackit.query.module.StaticContext;
import io.brackit.query.compiler.Bits;
import io.sirix.query.function.sdb.SDBFun;
import io.sirix.query.function.xml.XMLFun;
import java.util.Set;

final class ReadOnlyQuery {
  private static final Set<String> STORAGE_WRITES = Set.of("store", "load", "import", "create", "drop", "mkdir",
      "drop-database", "drop-resource", "create-cas-index", "create-name-index", "create-path-index",
      "create-valid-time-index", "drop-valid-time-index", "create-projection-index", "drop-projection-index",
      "create-temporal-aggregate-index", "drop-temporal-aggregate-index");

  private ReadOnlyQuery() {}

  static boolean updating(final String source) {
    final AST parsed = new JsoniqParser(source).parse();
    if (updating(parsed, null))
      return true;
    final var analyzer = new Analyzer(new BaseResolver(), null, parsed);
    return updating(analyzer.getAST(), analyzer.getModules().getFirst().getStaticContext());
  }

  private static boolean updating(final AST node, final StaticContext context) {
    switch (node.getType()) {
      case XQ.InsertExpr, XQ.DeleteExpr, XQ.ReplaceValueExpr, XQ.ReplaceNodeExpr, XQ.RenameExpr, XQ.TransformExpr,
          XQ.InsertJsonExpr, XQ.DeleteJsonExpr, XQ.RenameJsonExpr, XQ.ReplaceJsonExpr, XQ.AppendJsonExpr,
          XQ.ModuleImport, XQ.LiteralFuncItem -> {
        return true;
      }
      default -> {
      }
    }
    if (context != null && node.getType() == XQ.FunctionCall) {
      final QNm name = (QNm) node.getValue();
      final int arity = node.getChildCount();
      final var function = context.getFunctions().resolve(name, arity);
      if (function != null && (function.isUpdating() || writes(function.getName())))
        return true;
    }
    for (int index = 0; index < node.getChildCount(); index++) {
      if (updating(node.getChild(index), context))
        return true;
    }
    return false;
  }

  private static boolean writes(final QNm name) {
    final String namespace = name.getNamespaceURI();
    final String local = name.getLocalName();
    if (namespace.equals(XMLFun.XML_NSURI) && local.equals("serialize"))
      return true;
    if (namespace.equals(JSONFun.JSON_NSURI) || namespace.equals(XMLFun.XML_NSURI) || namespace.equals(Bits.BIT_NSURI))
      return STORAGE_WRITES.contains(local) || local.equals("eval");
    if (namespace.equals(SDBFun.SDB_NSURI))
      return local.equals("commit") || local.equals("rollback");
    if (namespace.equals(IOFun.IO_NSURI))
      return local.equals("write") || local.equals("writeline");
    return namespace.equals(Namespaces.FN_NSURI)
        && (local.equals("function-lookup") || local.equals("load-xquery-module"));
  }
}
