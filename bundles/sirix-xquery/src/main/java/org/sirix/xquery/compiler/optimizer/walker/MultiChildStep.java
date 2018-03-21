package org.sirix.xquery.compiler.optimizer.walker;

/*
 * [New BSD License] Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org> All rights
 * reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the Brackit Project Team nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.compiler.optimizer.walker.Walker;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.util.Cfg;
import org.sirix.xquery.compiler.XQExt;

/**
 * @author Sebastian Baechle
 * 
 */
public class MultiChildStep extends Walker {

  private static final int MIN_CHILD_STEP_LENGTH =
      Cfg.asInt("org.sirix.xquery.optimize.multichild.length", 3);

  public MultiChildStep(StaticContext sctx) {
    super(sctx);
  }

  @Override
  protected AST visit(AST node) {
    if (node.getType() != XQ.PathExpr) {
      return node;
    }

    snapshot();
    boolean checkInput = false;
    boolean skipDDO = false;
    int len = 0;

    for (int i = 1; i < node.getChildCount(); i++) {
      AST step = node.getChild(i);
      boolean childStep = ((step.getType() == XQ.StepExpr) && (getAxis(step) == XQ.CHILD));
      boolean hasPredicate = (step.getChildCount() > 2);
      if (childStep) {
        skipDDO |= step.checkProperty("skipDDO");
        checkInput |= step.checkProperty("checkInput");
        if (!hasPredicate) {
          len++;
        } else {
          if (len > MIN_CHILD_STEP_LENGTH) {
            merge(node, i - len, len, skipDDO, checkInput);
            i -= len;
          }
          checkInput = false;
          skipDDO = false;
          len = 0;
        }
      } else {
        if (len > MIN_CHILD_STEP_LENGTH) {
          merge(node, i - len, len - 1, skipDDO, checkInput);
          i -= len - 1;
        }
        checkInput = false;
        skipDDO = false;
        len = 0;
      }
    }
    if (len > MIN_CHILD_STEP_LENGTH) {
      merge(node, node.getChildCount() - len, len - 1, skipDDO, checkInput);
    }

    return node;
  }

  private void merge(AST node, int start, int len, boolean skipDDO, boolean checkInput) {
    AST multistep = new AST(XQExt.MultiStepExpr, XQExt.toName(XQExt.MultiStepExpr));
    multistep.setProperty("skipDDO", skipDDO);
    multistep.setProperty("checkInput", checkInput);
    for (int j = 0; j <= len; j++) {
      AST pstep = node.getChild(start);
      for (int k = 0; k < pstep.getChildCount(); k++) {
        multistep.addChild(pstep.getChild(k).copyTree());
      }
      node.deleteChild(start);
    }
    node.insertChild(start, multistep);
    snapshot();
  }

  public static void main(String[] args) throws Exception {
    // new XQuery(new DBCompileChain(null, null),
    // "let $a := <x/> return $a/b/c/d//e/x/y/z//u/v/w");
    // new XQuery(new DBCompileChain(null, null),
    // "let $a := <x/> return $a/b/@aha");
  }

  private int getAxis(AST stepExpr) {
    return stepExpr.getChild(0).getChild(0).getType();
  }
}
