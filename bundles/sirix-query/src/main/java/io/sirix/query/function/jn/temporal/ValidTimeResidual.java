package io.sirix.query.function.jn.temporal;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Bool;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.Cmp;
import io.brackit.query.atomic.QNm;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Type;
import io.brackit.query.module.StaticContext;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.jdm.json.Object;

import java.util.function.Predicate;
import java.util.function.Supplier;

/** The original dateTime comparison, retained for intervals the index cannot prove exactly. */
final class ValidTimeResidual implements Predicate<Item> {
  private final StaticContext context;
  private final QueryContext queryContext;
  private final Supplier<Sequence> point;
  private final QNm field;
  private final Cmp comparison;
  private final boolean general;
  private final boolean fieldOnLeft;

  ValidTimeResidual(final StaticContext context, final QueryContext queryContext, final Supplier<Sequence> point,
      final String field, final boolean start, final boolean strict, final boolean general, final boolean fieldOnLeft) {
    this.context = context;
    this.queryContext = queryContext;
    this.point = point;
    this.field = new QNm(field);
    final Cmp ordered = strict ? Cmp.lt : Cmp.le;
    comparison = start == fieldOnLeft ? ordered : ordered.swap();
    this.general = general;
    this.fieldOnLeft = fieldOnLeft;
  }

  @Override
  public boolean test(final Item object) {
    final Bool result;
    if (general && fieldOnLeft) {
      final Item bound = bound(object);
      result = comparison.gCmpAsBool(queryContext, bound, point.get());
    } else if (general) {
      final Sequence value = point.get();
      result = comparison.gCmpAsBool(queryContext, value, bound(object));
    } else if (fieldOnLeft) {
      final Item bound = bound(object);
      result = comparison.vCmpAsBool(queryContext, bound, ExprUtil.asItem(point.get()));
    } else {
      final Item value = ExprUtil.asItem(point.get());
      result = comparison.vCmpAsBool(queryContext, value, bound(object));
    }
    return result != null && result.booleanValue();
  }

  private Item bound(final Item item) {
    final Item value = item instanceof Object object ? ExprUtil.asItem(object.get(field)) : null;
    return value == null ? null : Cast.cast(context, value, Type.DATI, true);
  }
}
