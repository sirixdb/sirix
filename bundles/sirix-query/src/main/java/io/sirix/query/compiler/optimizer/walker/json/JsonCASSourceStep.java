package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.compiler.optimizer.walker.Walker;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.sirix.index.IndexType;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.function.jn.io.Doc;
import io.sirix.query.function.jn.io.DocByPointInTime;
import io.sirix.query.json.JsonDBStore;

import java.util.ArrayDeque;

/** Routes literal CAS equalities without consuming the FLWOR/filter residual. */
public final class JsonCASSourceStep extends Walker {
  private final JsonDBStore store;

  public JsonCASSourceStep(final JsonDBStore store) {
    this.store = store;
  }

  @Override
  protected AST visit(final AST node) {
    final AST source;
    final AST predicate;
    final Object variable;
    final int sourceIndex;
    if (node.getType() == XQ.ForBind && node.getChildCount() == 3 && !node.checkProperty("allowingEmpty")
        && node.getChild(0).getChildCount() == 1 && node.getChild(2).getType() == XQ.Selection) {
      sourceIndex = 1;
      source = node.getChild(1);
      predicate = node.getChild(2).getChild(0);
      variable = node.getChild(0).getChild(0).getValue();
    } else if (node.getType() == XQ.FilterExpr && node.getChildCount() == 2
        && node.getChild(1).getType() == XQ.Predicate && node.getChild(1).getChildCount() == 1) {
      sourceIndex = 0;
      source = node.getChild(0);
      predicate = node.getChild(1).getChild(0);
      variable = null;
    } else {
      return node;
    }
    if (source.getType() != XQ.ArrayAccess || source.getChildCount() != 2
        || source.getChild(1).getType() != XQ.SequenceExpr || source.getChild(1).getChildCount() != 0) {
      return node;
    }
    final var names = new ArrayDeque<QNm>();
    AST document = source.getChild(0);
    while (document.getType() == XQ.DerefExpr && document.getChildCount() == 2
        && document.getChild(1).getValue() instanceof QNm field) {
      names.addFirst(field);
      document = document.getChild(0);
    }
    if (document.getType() != XQ.FunctionCall
        || !(Doc.DOC.equals(document.getValue()) || DocByPointInTime.OPEN.equals(document.getValue()))
        || document.getChildCount() < 2 || document.getChildCount() > 3
        || !(document.getChild(0).getValue() instanceof Str database)
        || !(document.getChild(1).getValue() instanceof Str resource)) {
      return node;
    }
    if (document.getChildCount() == 3 && !RevisionData.isStableOperand(document.getChild(2))) {
      return node;
    }
    final AST equality = equality(predicate, variable);
    if (equality == null) {
      return node;
    }
    final Atomic atomic = (Atomic) equality.getChild(2).getValue();
    final var path = new Path<QNm>();
    for (final QNm name : names) {
      path.childObjectField(name);
    }
    path.childArray().childObjectField(new QNm(equality.getChild(1).getChild(1).getStringValue()));
    final var collection = store.lookup(database.stringValue());
    if (collection == null) {
      return node;
    }
    // Borrow the store-owned session. Historical availability is checked again at execution.
    final var session = collection.getDatabase().beginResourceSession(resource.stringValue());
    final Type type = atomic.type().instanceOf(Type.INR)
        ? Type.INR
        : atomic.type();
    if (session.getRtxIndexController(session.getMostRecentRevisionNumber())
               .getIndexes()
               .findCASIndex(path, type)
               .isEmpty()) {
      return node;
    }
    final AST index = new AST(XQExt.IndexExpr, XQExt.toName(XQExt.IndexExpr));
    index.setProperty("databaseName", database.stringValue());
    index.setProperty("resourceName", resource.stringValue());
    index.setProperty("revision", -1);
    index.setProperty("indexType", IndexType.CAS);
    index.setProperty("casSourcePath", path);
    index.setProperty("casSourceType", type);
    index.setProperty("atomic", atomic);
    index.setProperty("revisionByInstant", DocByPointInTime.OPEN.equals(document.getValue()));
    index.addChild(document.getChildCount() == 3
        ? document.getChild(2).copyTree()
        : new AST(XQ.Int, new Int32(-1)));
    index.addChild(source.copyTree());
    node.replaceChild(sourceIndex, index);
    return node;
  }

  private static AST equality(final AST node, final Object variable) {
    if (node.getType() == XQ.AndExpr) {
      for (int i = 0; i < node.getChildCount(); i++) {
        final AST match = equality(node.getChild(i), variable);
        if (match != null) {
          return match;
        }
      }
      return null;
    }
    if (node.getType() != XQ.ComparisonExpr || node.getChildCount() != 3
        || !(node.getChild(0).getType() == XQ.ValueCompEQ || node.getChild(0).getType() == XQ.GeneralCompEQ)
        || !(node.getChild(2).getValue() instanceof Atomic)) {
      return null;
    }
    final AST field = node.getChild(1);
    if (field.getType() != XQ.DerefExpr || field.getChildCount() != 2
        || !(field.getChild(1).getValue() instanceof QNm)) {
      return null;
    }
    final AST reference = field.getChild(0);
    return (variable == null
        ? reference.getType() == XQ.ContextItemExpr
        : reference.getType() == XQ.VariableRef && variable.equals(reference.getValue()))
            ? node
            : null;
  }
}
