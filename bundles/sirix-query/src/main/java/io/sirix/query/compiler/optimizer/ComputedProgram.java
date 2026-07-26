package io.sirix.query.compiler.optimizer;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared postfix-program builder for COMPUTED expressions over covered numeric fields
 * (gap item 2): {@code +,-,*} trees of direct {@code $r.field} derefs and integer
 * literals. Slots {@code >= 0 && < CONST_BASE} push the operand field with that index;
 * slots {@code >= CONST_BASE} push {@code consts[slot - CONST_BASE]};
 * {@link #OP_ADD}/{@link #OP_SUB}/{@link #OP_MUL} pop two, push the result. The serving
 * kernels evaluate with {@code Math.*Exact} and DECLINE on overflow (the interpreter
 * promotes to exact decimal). Opcode values mirror
 * {@code ProjectionIndexByteScan.COMPUTED_OP_*} — sirix-core cannot depend on this
 * module, so the pairing is by documented contract.
 */
public final class ComputedProgram {

  public static final int OP_ADD = -1;
  public static final int OP_SUB = -2;
  public static final int OP_MUL = -3;
  public static final int CONST_BASE = 1 << 20;

  private static final int MAX_CODE = 64;
  private static final int MAX_FIELDS = 8;
  private static final int MAX_CONSTS = 16;

  /** Compiled program; operand indexes refer to the caller's SHARED fields list. */
  public record Program(int[] code, long[] consts) {
  }

  private ComputedProgram() {
  }

  /**
   * Compile {@code expr} into a postfix program, interning operand fields into the
   * caller-owned {@code fields} list (shared across a record's entries so the kernel
   * loads each column once); code and constants are per program. {@code null} =
   * unservable shape (unsupported operator, non-literal operand, foreign variable, size
   * caps) — interned fields are rolled back.
   */
  public static Program build(final AST expr, final QNm loopVar, final List<String> fields) {
    final int fieldsMark = fields.size();
    final ArrayList<Long> consts = new ArrayList<>(4);
    final ArrayList<Integer> code = new ArrayList<>(8);
    if (!emit(expr, loopVar, fields, consts, code)) {
      trim(fields, fieldsMark);
      return null;
    }
    // A well-formed binary postfix program: running depth never dips below 1 and ends at 1.
    int depth = 0;
    for (final int c : code) {
      depth += c >= 0 ? 1 : -1;
      if (depth < 1) {
        trim(fields, fieldsMark);
        return null;
      }
    }
    if (depth != 1) {
      trim(fields, fieldsMark);
      return null;
    }
    final int[] codeArr = new int[code.size()];
    for (int i = 0; i < codeArr.length; i++) {
      codeArr[i] = code.get(i);
    }
    final long[] constArr = new long[consts.size()];
    for (int i = 0; i < constArr.length; i++) {
      constArr[i] = consts.get(i);
    }
    return new Program(codeArr, constArr);
  }

  private static void trim(final List<?> list, final int mark) {
    while (list.size() > mark) {
      list.remove(list.size() - 1);
    }
  }

  private static boolean emit(final AST expr, final QNm loopVar, final List<String> fields,
      final List<Long> consts, final List<Integer> code) {
    if (expr == null || code.size() >= MAX_CODE) {
      return false;
    }
    switch (expr.getType()) {
      case XQ.ArithmeticExpr -> {
        if (expr.getChildCount() != 3) {
          return false;
        }
        final int op = switch (expr.getChild(0).getType()) {
          case XQ.AddOp -> OP_ADD;
          case XQ.SubtractOp -> OP_SUB;
          case XQ.MultiplyOp -> OP_MUL;
          default -> 0; // div/idiv/mod: decimal/rounding semantics — decline
        };
        if (op == 0) {
          return false;
        }
        if (!emit(expr.getChild(1), loopVar, fields, consts, code)
            || !emit(expr.getChild(2), loopVar, fields, consts, code)) {
          return false;
        }
        if (code.size() >= MAX_CODE) {
          return false;
        }
        code.add(op);
        return true;
      }
      case XQ.DerefExpr -> {
        final String field = loopVarDerefField(expr, loopVar);
        if (field == null) {
          return false;
        }
        int idx = fields.indexOf(field);
        if (idx < 0) {
          // >= not ==: the row-materialize path shares a fields list that DIRECT record
          // entries grow past the cap before any program interns — equality would let
          // computed operands blow straight through the bound.
          if (fields.size() >= MAX_FIELDS) {
            return false;
          }
          idx = fields.size();
          fields.add(field);
        }
        code.add(idx);
        return true;
      }
      case XQ.Int -> {
        final Object v = expr.getValue();
        final long lit;
        if (v instanceof Int32 i32) {
          lit = i32.longValue();
        } else if (v instanceof Int64 i64) {
          lit = i64.longValue();
        } else {
          return false; // Int node carrying a big-integer atomic — decline
        }
        if (consts.size() == MAX_CONSTS) {
          return false;
        }
        code.add(CONST_BASE + consts.size());
        consts.add(lit);
        return true;
      }
      default -> {
        return false;
      }
    }
  }

  /** {@code $loopVar.field} direct deref → field local name, else {@code null}. */
  static String loopVarDerefField(final AST expr, final QNm loopVar) {
    if (expr == null || expr.getType() != XQ.DerefExpr || expr.getChildCount() < 2) {
      return null;
    }
    final AST base = expr.getChild(0);
    if (base.getType() != XQ.VariableRef || !loopVar.equals(base.getValue())) {
      return null;
    }
    final Object name = expr.getChild(expr.getChildCount() - 1).getValue();
    if (name instanceof QNm qnm) {
      return qnm.getLocalName();
    }
    return name instanceof String s ? s : null;
  }
}
