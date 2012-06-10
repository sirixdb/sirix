/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.xpath.parser;

import java.util.Arrays;
import java.util.NoSuchElementException;

import org.sirix.api.IAxis;
import org.sirix.api.IFilter;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AbsAxis;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.AttributeAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.FilterAxis;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.FollowingSiblingAxis;
import org.sirix.axis.ParentAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.axis.PrecedingSiblingAxis;
import org.sirix.axis.SelfAxis;
import org.sirix.axis.filter.AttributeFilter;
import org.sirix.axis.filter.CommentFilter;
import org.sirix.axis.filter.DocumentRootNodeFilter;
import org.sirix.axis.filter.ElementFilter;
import org.sirix.axis.filter.ItemFilter;
import org.sirix.axis.filter.NameFilter;
import org.sirix.axis.filter.NestedFilter;
import org.sirix.axis.filter.NodeFilter;
import org.sirix.axis.filter.PIFilter;
import org.sirix.axis.filter.TextFilter;
import org.sirix.axis.filter.TypeFilter;
import org.sirix.axis.filter.WildcardFilter;
import org.sirix.axis.filter.WildcardFilter.EType;
import org.sirix.exception.TTXPathException;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.service.xml.xpath.PipelineBuilder;
import org.sirix.service.xml.xpath.SequenceType;
import org.sirix.service.xml.xpath.SingleType;
import org.sirix.service.xml.xpath.filter.DocumentNodeAxis;
import org.sirix.service.xml.xpath.filter.SchemaAttributeFilter;
import org.sirix.service.xml.xpath.filter.SchemaElementFilter;
import org.sirix.utils.TypedValue;

/**
 * <h1>XPath Parser</h1>
 * <p>
 * Parses the given XPath query and starts the execution of the XPath request. The given query is send to the
 * scanner that categorizes the symbols by creating tokens. The parser receives this tokens, checks the
 * grammar according to the EBNF given on <a href="http://www.w3.org/TR/xquery-xpath-parsing/">
 * http://www.w3.org/TR/xquery-xpath-parsing/</a>.Then it constitutes the query execution chain.
 * </p>
 */
public final class XPathParser {

  /** IReadTransaction to access the nodes. Is needed for filters and axes. */
  private final INodeReadTrx mRTX;

  /** Scanner that scans the symbols of the query and returns them as tokens. */
  private final XPathScanner mScanner;

  /** Represents the current read token. */
  private IXPathToken mToken;

  /**
   * Builds the chain of nested IAxis that evaluate the query in a pipeline
   * manner.
   */
  private final PipelineBuilder mPipeBuilder;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx
   *          The transaction.
   * @param mQuery
   *          The query to process.
   */
  public XPathParser(final INodeReadTrx rtx, final String mQuery) {

    mRTX = rtx;
    mScanner = new XPathScanner(mQuery);
    mPipeBuilder = new PipelineBuilder();

  }

  /**
   * Starts parsing the query.
   * 
   * @throws TTXPathException
   */
  public void parseQuery() throws TTXPathException {

    // get first token, ignore all white spaces
    do {
      mToken = mScanner.nextToken();
    } while (mToken.getType() == TokenType.SPACE);

    // parse the query according to the rules specified in the XPath 2.0 REC
    parseExpression();

    // after the parsing of the expression no token must be left
    if (mToken.getType() != TokenType.END) {
      throw new IllegalStateException("The query has not been processed completely.");
    }
  }

  /**
   * Parses an XPath expression according to the following EBNF production
   * rule:
   * <p>
   * [2] Expr ::= ExprSingle ("," ExprSingle)* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseExpression() throws TTXPathException {
    mPipeBuilder.addExpr();

    int no = 0;
    do {
      parseExprSingle();
      no++;

    } while (is(TokenType.COMMA, true));

    mPipeBuilder.finishExpr(getTransaction(), no);
  }

  /**
   * Parses the the rule ExprSingle according to the following production
   * rule:
   * <p>
   * [3] ExprSingle ::= ForExpr | QuantifiedExpr | IfExpr | OrExpr .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseExprSingle() throws TTXPathException {

    mPipeBuilder.addExpressionSingle();

    final String tContent = mToken.getContent();
    if ("for".equals(tContent)) {
      parseForExpr();
    } else if ("some".equals(tContent) || "every".equals(tContent)) {
      parseQuantifiedExpr();
    } else if ("if".equals(tContent)) {
      parseIfExpr();
    } else {
      parseOrExpr();
    }
  }

  /**
   * Parses the the rule ForExpr according to the following production rule:
   * [4]
   * <p>
   * ForExpr ::= SimpleForClause "return" ExprSingle .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseForExpr() throws TTXPathException {

    // get number of all for conditions
    final int rangeVarNo = parseSimpleForClause();
    consume("return", true);

    // parse return clause
    parseExprSingle();

    mPipeBuilder.addForExpression(rangeVarNo);

  }

  /**
   * Parses the the rule SimpleForClause according to the following production
   * rule:
   * <p>
   * [5] SimpleForClause ::= <"for" "$"> VarName "in" ExprSingle ("," "$" VarName "in" ExprSingle)* .
   * </p>
   * 
   * @return returns the number of for-conditions
   * @throws TTXPathException
   */
  private int parseSimpleForClause() throws TTXPathException {

    consume("for", true);

    int forCondNo = 0;

    // parse all conditions and count them
    do {

      consume(TokenType.DOLLAR, true);

      // parse for variables
      final String varName = parseVarName();
      consume("in", true);

      parseExprSingle();
      mPipeBuilder.addVariableExpr(getTransaction(), varName);
      forCondNo++;

    } while (is(TokenType.COMMA, true));

    return forCondNo;

  }

  /**
   * Parses the the rule QuantifiedExpr according to the following production
   * rule:
   * <p>
   * [6] QuantifiedExpr ::= (<"some" "$"> | <"every" "$">) VarName "in" ExprSingle ("," "$" VarName "in"
   * ExprSingle)* "satisfies" ExprSingle .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseQuantifiedExpr() throws TTXPathException {

    // identify quantifier type
    final boolean isSome = is("some", true);
    if (!isSome) {
      consume("every", true);
    }

    // count number of variables
    int varNo = 0;

    do {

      // parse variable name
      consume(TokenType.DOLLAR, true);
      final String varName = parseVarName();
      consume("in", true);
      varNo++;

      parseExprSingle();
      mPipeBuilder.addVariableExpr(getTransaction(), varName);

    } while (is(TokenType.COMMA, true));

    // parse satisfies expression
    consume("satisfies", true);

    parseExprSingle();

    mPipeBuilder.addQuantifierExpr(getTransaction(), isSome, varNo);

  }

  /**
   * Parses the the rule IfExpr according to the following production rule:
   * <p>
   * [7] IfExpr ::= <"if" "("> Expr ")" "then" ExprSingle "else" ExprSingle.
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseIfExpr() throws TTXPathException {

    // parse if expression
    consume("if", true);
    consume(TokenType.OPEN_BR, true);

    // parse test expression axis
    parseExpression();
    consume(TokenType.CLOSE_BR, true);

    // parse then expression
    consume("then", true);
    parseExprSingle();

    // parse else expression
    consume("else", true);
    parseExprSingle();

    mPipeBuilder.addIfExpression(getTransaction());

  }

  /**
   * Parses the the rule OrExpr according to the following production rule:
   * <p>
   * [8] OrExpr ::= AndExpr ( "or" AndExpr )* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseOrExpr() throws TTXPathException {

    parseAndExpr();

    while (is("or", true)) {

      mPipeBuilder.addExpressionSingle();

      parseAndExpr();
      mPipeBuilder.addOrExpression(getTransaction());

    }
  }

  /**
   * Parses the the rule AndExpr according to the following production rule:
   * <p>
   * [9] AndExpr ::= ComparisonExpr ( "and" ComparisonExpr )* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseAndExpr() throws TTXPathException {

    parseComparisionExpr();

    while (is("and", true)) {

      mPipeBuilder.addExpressionSingle();

      parseComparisionExpr();
      mPipeBuilder.addAndExpression(getTransaction());

    }
  }

  /**
   * Parses the the rule ComparisionExpr according to the following production
   * rule:
   * <p>
   * [10] ComparisonExpr ::= RangeExpr ( (ValueComp | GeneralComp | NodeComp) RangeExpr )? .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseComparisionExpr() throws TTXPathException {

    parseRangeExpr();

    final String mComp = mToken.getContent();

    if (isComp()) {

      // parse second operator axis
      mPipeBuilder.addExpressionSingle();

      parseRangeExpr();

      mPipeBuilder.addCompExpression(getTransaction(), mComp);
    }

  }

  /**
   * Indicates whether the current token is a comparison operator, or not. *
   * Parses the the rule NodeComp [24], ValueComp [23] and GeneralComp [22]
   * according to the following production rule:
   * <p>
   * [22] GeneralComp ::= * "=" | "!=" | "<" | "<=" | * ">" | ">=" .
   * </p>
   * <p>
   * [23] ValueComp ::= "eq" | "ne" | "lt" | "le" | "gt" | "ge" .
   * </p>
   * <p>
   * [24] NodeComp ::= "is" | "<<" | ">>" .
   * </p>
   * <p>
   * [22-24] Comp ::= "=" | "!=" | "<" | "<=" | ">" | ">=" |"eq" | "ne" | "lt" | "le" | "gt" | "ge" | "is" |
   * "<<" | ">>" .
   * </p>
   * 
   * @return true, if the current token is a comparison operator.
   */
  private boolean isComp() {

    return is(TokenType.L_SHIFT, true)
      || is(TokenType.R_SHIFT, true)
      || is(TokenType.EQ, true)
      || is(TokenType.N_EQ, true)
      || is(TokenType.COMP, true)
      || mToken.getType() == TokenType.TEXT
      && (is("ne", true) || is("eq", true) || is("lt", true) || is("le", true) || is("gt", true)
        || is("ge", true) || is("is", true));
  }

  /**
   * Parses the the rule RangeExpr according to the following production rule:
   * <p>
   * [11] RangeExpr ::= AdditiveExpr ( "to" AdditiveExpr )? .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseRangeExpr() throws TTXPathException {

    parseAdditiveExpr();
    if (is("to", true)) {

      mPipeBuilder.addExpressionSingle();

      parseAdditiveExpr();
      mPipeBuilder.addRangeExpr(getTransaction());

    }
  }

  /**
   * Parses the the rule AdditiveExpr according to the following production
   * rule:
   * <p>
   * [12] AdditiveExpr ::= MultiplicativeExpr(("+" | "-") MultiplicativeExpr)* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseAdditiveExpr() throws TTXPathException {

    parseMultiplicativeExpr();

    String op = mToken.getContent();
    while (is(TokenType.PLUS, true) || is(TokenType.MINUS, true)) {

      // identify current operator kind
      // for (Operators op : Operators.values()) {
      // if (is(op.getOpName(), true)) {

      // parse second operand axis

      mPipeBuilder.addExpressionSingle();

      parseMultiplicativeExpr();

      mPipeBuilder.addOperatorExpression(getTransaction(), op);
      op = mToken.getContent();
    }
    // }
    // }
  }

  /**
   * Parses the the rule MultiplicativeExpr according to the following
   * production rule:
   * <p>
   * [13] MultiplicativeExpr ::= UnionExpr ( ("*" | "div" | "idiv" | "mod") UnionExpr )* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseMultiplicativeExpr() throws TTXPathException {

    parseUnionExpr();

    String op = mToken.getContent();
    while (isMultiplication()) {

      // for (Operators op : Operators.values()) {
      // // identify current operator
      // if (is(op.getOpName(), true)) {

      mPipeBuilder.addExpressionSingle();

      // parse second operand axis
      parseUnionExpr();

      mPipeBuilder.addOperatorExpression(getTransaction(), op);
      op = mToken.getContent();
      // }
      // }
    }
  }

  /**
   * Parses the the rule UnionExpr according to the following production rule:
   * <p>
   * [14] UnionExpr ::= IntersectExceptExpr ( ("union" | "|") IntersectExceptExpr )* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseUnionExpr() throws TTXPathException {

    parseIntersectExceptExpr();

    while (is("union", true) || is(TokenType.OR, true)) {

      // parse second operand axis

      mPipeBuilder.addExpressionSingle();

      parseIntersectExceptExpr();

      mPipeBuilder.addUnionExpression(getTransaction());

    }
  }

  /**
   * Parses the the rule IntersectExceptExpr according to the following
   * production rule:
   * <p>
   * [15] IntersectExceptExpr ::= InstanceofExpr ( ("intersect" | * "except") InstanceofExpr )* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseIntersectExceptExpr() throws TTXPathException {

    parseInstanceOfExpr();

    boolean isIntersect = mToken.getContent().equals("intersect");

    while (is("intersect", true) || is("except", true)) {
      // parse second operand axis

      mPipeBuilder.addExpressionSingle();

      parseInstanceOfExpr();

      mPipeBuilder.addIntExcExpression(getTransaction(), isIntersect);

      isIntersect = mToken.getContent().equals("intersect");
    }

  }

  /**
   * Parses the the rule InstanceOfExpr according to the following production
   * rule:
   * <p>
   * [16] InstanceofExpr ::= TreatExpr ( <"instance" "of"> SequenceType )?.
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseInstanceOfExpr() throws TTXPathException {

    parseTreatExpr();

    if (is("instance", true)) {
      consume("of", true);

      mPipeBuilder.addInstanceOfExpr(getTransaction(), parseSequenceType());
    }
  }

  /**
   * Parses the the rule TreatExpr according to the following production rule:
   * <p>
   * [17] TreatExpr ::= CastableExpr ( <"treat" "as"> SequenceType )? .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseTreatExpr() throws TTXPathException {

    parseCastableExpr();
    if (is("treat", true)) {
      consume("as", true);
      mPipeBuilder.addTreatExpr(getTransaction(), parseSequenceType());
    }
  }

  /**
   * Parses the the rule CastableExpr according to the following production
   * rule:
   * <p>
   * [18] CastableExpr ::= CastExpr ( <"castable" "as"> SingleType )? .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseCastableExpr() throws TTXPathException {

    parseCastExpr();
    if (is("castable", true)) {

      consume("as", true);
      mPipeBuilder.addCastableExpr(getTransaction(), parseSingleType());

    }
  }

  /**
   * Parses the the rule CastExpr according to the following production rule:
   * <p>
   * [19] CastExpr ::= UnaryExpr ( <"cast" "as"> SingleType )? .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseCastExpr() throws TTXPathException {

    parseUnaryExpr();
    if (is("cast", true)) {

      consume("as", true);
      mPipeBuilder.addCastExpr(getTransaction(), parseSingleType());
    }
  }

  /**
   * Parses the the rule UnaryExpr according to the following production rule:
   * <p>
   * [20] UnaryExpr ::= ("-" | "+")* ValueExpr .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseUnaryExpr() throws TTXPathException {

    boolean isUnaryMinus = false;

    // the plus can be ignored since it does not modify the result
    while (is(TokenType.PLUS, true) || mToken.getType() == TokenType.MINUS) {

      if (is(TokenType.MINUS, true)) {
        // two following minuses is a plus and therefore can be ignored,
        // thus only in case of an odd number of minus signs, the unary
        // operation
        // has to be processed
        isUnaryMinus = !isUnaryMinus;
      }
    }

    if (isUnaryMinus) {
      // unary minus has to be processed

      mPipeBuilder.addExpressionSingle();

      parseValueExpr();
      mPipeBuilder.addOperatorExpression(getTransaction(), "unary");

    } else {

      parseValueExpr();
    }
  }

  /**
   * Parses the the rule ValueExpr according to the following production rule:
   * <p>
   * [21] ValueExpr ::= PathExpr .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseValueExpr() throws TTXPathException {

    parsePathExpr();
  }

  /**
   * Parses the the rule PathExpr according to the following production rule:
   * <p>
   * [25] PathExpr ::= ("/" RelativePathExpr?) | ("//" RelativePathExpr) | RelativePathExpr .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parsePathExpr() throws TTXPathException {

    if (is(TokenType.SLASH, true)) {
      // path expression starts from the root
      mPipeBuilder.addStep(new DocumentNodeAxis(getTransaction()));
      final TokenType type = mToken.getType();

      if (type != TokenType.END && type != TokenType.COMMA) {
        // all immediately following keywords or '*' are nametests, not
        // operators
        // leading-lone-slash constrain

        parseRelativePathExpr();
      }

    } else if (is(TokenType.DESC_STEP, true)) {
      // path expression starts from the root with a descendant-or-self
      // step
      mPipeBuilder.addStep(new DocumentNodeAxis(getTransaction()));

      final IAxis mAxis = new DescendantAxis(getTransaction(), EIncludeSelf.YES);

      mPipeBuilder.addStep(mAxis);

      parseRelativePathExpr();
    } else {
      parseRelativePathExpr();
    }
  }

  /**
   * Parses the the rule RelativePathExpr according to the following
   * production rule:
   * <p>
   * [26] RelativePathExpr ::= StepExpr (("/" | "//") StepExpr)* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseRelativePathExpr() throws TTXPathException {
    parseStepExpr();

    while (mToken.getType() == TokenType.SLASH || mToken.getType() == TokenType.DESC_STEP) {
      if (is(TokenType.DESC_STEP, true)) {
        final IAxis axis = new DescendantAxis(getTransaction(), EIncludeSelf.YES);
        mPipeBuilder.addStep(axis);
      } else {
        // in this case the slash is just a separator
        consume(TokenType.SLASH, true);
      }
      parseStepExpr();
    }
  }

  /**
   * Parses the the rule StepExpr according to the following production rule:
   * <p>
   * [27] StepExpr ::= AxisStep | FilterExpr .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseStepExpr() throws TTXPathException {
    if (isFilterExpr()) {
      parseFilterExpr();
    } else {
      parseAxisStep();
    }
  }

  /**
   * @return true, if current token is part of a filter expression
   */
  private boolean isFilterExpr() {
    final TokenType type = mToken.getType();
    return type == TokenType.DOLLAR || type == TokenType.POINT || type == TokenType.OPEN_BR
      || isFunctionCall() || isLiteral();
  }

  /**
   * The current token is part of a function call, if it is followed by a open
   * braces (current token is name of the function), or is followed by a colon
   * that is followed by a name an a open braces (current token is prefix of
   * the function name.).
   * 
   * @return true, if the current token is part of a function call
   */
  private boolean isFunctionCall() {

    return mToken.getType() == TokenType.TEXT
      && (!isReservedKeyword())
      && (mScanner.lookUpTokens(1).getType() == TokenType.OPEN_BR || (mScanner.lookUpTokens(1).getType() == TokenType.COLON && mScanner
        .lookUpTokens(3).getType() == TokenType.OPEN_BR));
  }

  /**
   * Although XPath is supposed to have no reserved words, some keywords are
   * not allowed as function names in an unprefixed form because expression
   * syntax takes precedence.
   * 
   * @return true if the token is one of the reserved words of XPath 2.0
   */
  private boolean isReservedKeyword() {

    final String content = mToken.getContent();
    return isKindTest() || "item".equals(content) || "if".equals(content) || "empty-sequence".equals(content)
      || "typeswitch".equals(content);
  }

  /**
   * Parses the the rule AxisStep according to the following production rule:
   * <p>
   * [28] AxisStep ::= (ForwardStep | ReverseStep) PredicateList .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseAxisStep() throws TTXPathException {

    if (isReverceStep()) {
      parseReverceStep();
    } else {
      parseForwardStep();
    }
    parsePredicateList();

  }

  /**
   * Parses the the rule ForwardStep according to the following production
   * rule:
   * <p>
   * [29] ForwardStep ::= (ForwardAxis NodeTest) | AbbrevForwardStep .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseForwardStep() throws TTXPathException {

    IAxis axis;
    IFilter filter;
    if (isForwardAxis()) {
      axis = parseForwardAxis();
      filter = parseNodeTest(axis.getClass() == AttributeAxis.class);

      mPipeBuilder.addStep(axis, filter);
    } else {
      axis = parseAbbrevForwardStep();

      mPipeBuilder.addStep(axis);
    }
  }

  /**
   * Parses the the rule ForwardAxis according to the following production
   * rule:
   * <p>
   * [30] ForwardAxis ::= <"child" "::"> | <"descendant" "::"> | <"attribute" "::"> | <"self" "::"> |
   * <"descendant-or-self" "::"> | <"following-sibling" "::"> | <"following" "::"> | <"namespace" "::"> .
   * </p>
   * 
   * @return axis
   * @throws TTXPathException
   */
  private IAxis parseForwardAxis() throws TTXPathException {
    final IAxis axis;
    if (is("child", true)) {
      axis = new ChildAxis(getTransaction());
    } else if (is("descendant", true)) {
      axis = new DescendantAxis(getTransaction());
    } else if (is("descendant-or-self", true)) {
      axis = new DescendantAxis(getTransaction(), EIncludeSelf.YES);
    } else if (is("attribute", true)) {
      axis = new AttributeAxis(getTransaction());
    } else if (is("self", true)) {
      axis = new SelfAxis(getTransaction());
    } else if (is("following", true)) {
      axis = new FollowingAxis(getTransaction());
    } else if (is("following-sibling", true)) {
      axis = new FollowingSiblingAxis(getTransaction());
    } else {
      is("namespace", true);
      throw EXPathError.XPST0010.getEncapsulatedException();
    }

    consume(TokenType.COLON, true);
    consume(TokenType.COLON, true);

    return axis;
  }

  /**
   * Checks if a given token represents a ForwardAxis.
   * 
   * @return true if the token is a ForwardAxis
   */
  private boolean isForwardAxis() {

    final String content = mToken.getContent();
    return (mToken.getType() == TokenType.TEXT && ("child".equals(content) || ("descendant".equals(content)
      || "descendant-or-self".equals(content) || "attribute".equals(content) || "self".equals(content)
      || "following".equals(content) || "following-sibling".equals(content) || "namespace".equals(content))));
  }

  /**
   * Parses the the rule AbrevForwardStep according to the following
   * production rule:
   * <p>
   * [31] AbbrevForwardStep ::= "@"? NodeTest .
   * </p>
   * 
   * @return FilterAxis
   */
  private AbsAxis parseAbbrevForwardStep() {

    AbsAxis axis;
    boolean isAttribute;

    if (is(TokenType.AT, true) || mToken.getContent().equals("attribute")
      || mToken.getContent().equals("schema-attribute")) {
      // in case of .../attribute(..), or .../schema-attribute() the
      // default
      // axis
      // is the attribute axis
      axis = new AttributeAxis(getTransaction());
      isAttribute = true;
    } else {
      // default axis is the child axis
      axis = new ChildAxis(getTransaction());
      isAttribute = false;
    }

    final IFilter filter = parseNodeTest(isAttribute);

    return new FilterAxis(axis, filter);
  }

  /**
   * Parses the the rule ReverceStep according to the following production
   * rule:
   * <p>
   * [32] ReverseStep ::= (ReverseAxis NodeTest) | AbbrevReverseStep .
   * </p>
   */
  private void parseReverceStep() {

    AbsAxis axis;
    if (mToken.getType() == TokenType.PARENT) {
      axis = parseAbbrevReverseStep();

      mPipeBuilder.addStep(axis);
    } else {
      axis = parseReverceAxis();
      final IFilter filter = parseNodeTest(axis.getClass() == AttributeAxis.class);
      mPipeBuilder.addStep(axis, filter);
    }
  }

  /**
   * Parses the the rule ReverceAxis according to the following production
   * rule: [33] ReverseAxis ::= <"parent" "::"> | <"ancestor" "::"> |
   * <"preceding-sibling" "::">|<"preceding" "::">|<"ancestor-or-self" "::"> .
   * 
   * @return axis
   */
  private AbsAxis parseReverceAxis() {

    AbsAxis axis;
    if (is("parent", true)) {

      axis = new ParentAxis(getTransaction());

    } else if (is("ancestor", true)) {

      axis = new AncestorAxis(getTransaction());

    } else if (is("ancestor-or-self", true)) {

      axis = new AncestorAxis(getTransaction(), EIncludeSelf.YES);

    } else if (is("preceding", true)) {

      axis = new PrecedingAxis(getTransaction());

    } else {
      consume("preceding-sibling", true);

      axis = new PrecedingSiblingAxis(getTransaction());

    }

    consume(TokenType.COLON, true);
    consume(TokenType.COLON, true);

    return axis;
  }

  /**
   * Parses the the rule AbbrevReverceStep according to the following
   * production rule:
   * <p>
   * [34] AbbrevReverseStep ::= ".." .
   * </p>
   * 
   * @return ParentAxis
   */
  private AbsAxis parseAbbrevReverseStep() {

    consume(TokenType.PARENT, true);
    return new ParentAxis(getTransaction());
  }

  /**
   * @return true, if current token is part of an reverse axis step
   */
  private boolean isReverceStep() {

    final TokenType type = mToken.getType();
    final String content = mToken.getContent();

    return (type == TokenType.PARENT || (type == TokenType.TEXT && ("parent".equals(content)
      || "ancestor".equals(content) || "preceding".equals(content) || "preceding-sibling".equals(content) || "ancestor-or-self"
        .equals(content))));
  }

  /**
   * Parses the the rule NodeTest according to the following production rule:
   * <p>
   * [35] NodeTest ::= KindTest | NameTest .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseNodeTest(final boolean mIsAtt) {

    IFilter filter;
    if (isKindTest()) {
      filter = parseKindTest();
    } else {
      filter = parseNameTest(mIsAtt);
    }
    return filter;
  }

  /**
   * Parses the the rule NameTest according to the following production rule:
   * <p>
   * [36] NameTest ::= QName | Wildcard .
   * </p>
   * 
   * @param mIsAtt
   *          Attribute
   * @return filter
   */
  private IFilter parseNameTest(final boolean mIsAtt) {

    IFilter filter;
    if (isWildcardNameTest()) {

      filter = parseWildcard(mIsAtt);
    } else {
      filter = new NameFilter(getTransaction(), parseQName());
    }
    return filter;
  }

  /**
   * @return true, if has the structure of a name test containing a wildcard
   *         ("*" | < NCName ":" "*" > | < "*" ":" NCName >)
   */
  private boolean isWildcardNameTest() {

    return mToken.getType() == TokenType.STAR
      || (mToken.getType() == TokenType.TEXT && mScanner.lookUpTokens(1).getType() == TokenType.COLON && mScanner
        .lookUpTokens(2).getType() == TokenType.STAR);
  }

  /**
   * Parses the the rule Wildcard according to the following production rule:
   * <p>
   * [37] Wildcard ::= "*" | < NCName ":" "*" > | < "*" ":" NCName > .
   * <p>
   * 
   * @param pIsAtt
   *          Attribute
   * @return filter
   */
  private IFilter parseWildcard(final boolean pIsAtt) {
    IFilter filter;
    EType isName = EType.PREFIX;

    if (is(TokenType.STAR, true)) {
      if (is(TokenType.COLON, true)) {
        isName = EType.LOCALNAME; // < "*" ":" NCName > .
      } else {
        return pIsAtt // "*"
          ? new AttributeFilter(getTransaction()) : new ElementFilter(getTransaction());
      }
    }

    filter = new WildcardFilter(getTransaction(), parseNCName(), isName);
    if (isName == EType.PREFIX) { // < NCName ":" "*" >
      consume(TokenType.COLON, true);
      consume(TokenType.STAR, true);
    }
    return filter;
  }

  /**
   * Parses the the rule FilterExpr according to the following production
   * rule:
   * <p>
   * [38] FilterExpr ::= PrimaryExpr PredicateList .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseFilterExpr() throws TTXPathException {

    parsePrimaryExpr();
    parsePredicateList();
  }

  /**
   * Parses the the rule PredicateList according to the following production
   * rule:
   * <p>
   * [39] PredicateList ::= Predicate* .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parsePredicateList() throws TTXPathException {

    while (mToken.getType() == TokenType.OPEN_SQP) {
      parsePredicate();
    }
  }

  /**
   * Parses the the rule Predicate according to the following production rule:
   * <p>
   * [40] Predicate ::= "[" Expr "]" .
   * </p>
   * <p>
   * The whole predicate expression is build as a separate expression chain and is then inserted to the main
   * expression chain by a predicate filter.
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parsePredicate() throws TTXPathException {

    consume(TokenType.OPEN_SQP, true);

    mPipeBuilder.addExpressionSingle();

    parseExpression();

    consume(TokenType.CLOSE_SQP, true);

    mPipeBuilder.addPredicate(getTransaction());
  }

  /**
   * Parses the the rule PrimaryExpr according to the following production
   * rule:
   * <p>
   * [41] PrimaryExpr ::= Literal | VarRef | ParenthesizedExpr | ContextItemExpr | FunctionCall .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parsePrimaryExpr() throws TTXPathException {

    if (isLiteral()) {
      parseLiteral();
    } else if (mToken.getType() == TokenType.DOLLAR) {
      parseVarRef();
    } else if (mToken.getType() == TokenType.OPEN_BR) {
      parseParenthesizedExpr();
    } else if (mToken.getType() == TokenType.POINT) {
      parseContextItemExpr();
    } else if (!isReservedKeyword()) {
      parseFunctionCall();
    } else {
      throw new IllegalStateException("Found wrong token '" + mToken.getContent() + "'. "
        + " Token should be either a literal, a variable," + "a '(', a '.' or a function call.");
    }
  }

  /**
   * @return true, if the current token represents a literal
   */
  private boolean isLiteral() {

    final TokenType type = mToken.getType();
    return (type == TokenType.SINGLE_QUOTE || type == TokenType.DBL_QUOTE || type == TokenType.VALUE);
  }

  /**
   * Parses the the rule Literal according to the following production rule:
   * <p>
   * [42] Literal ::= NumericLiteral | StringLiteral .
   * </p>
   */
  private void parseLiteral() {

    int itemKey;

    if (mToken.getType() == TokenType.VALUE || mToken.getType() == TokenType.POINT) {
      // is numeric literal
      itemKey = parseNumericLiteral();
    } else {
      // is string literal
      assert (mToken.getType() == TokenType.DBL_QUOTE || mToken.getType() == TokenType.SINGLE_QUOTE);
      itemKey = parseStringLiteral();
    }

    mPipeBuilder.addLiteral(getTransaction(), itemKey);

  }

  /**
   * Parses the the rule NumericLiteral according to the following production
   * rule:
   * <p>
   * [43] NumericLiteral ::= IntegerLiteral | DecimalLiteral | DoubleLiteral .
   * </p>
   * 
   * @return parseIntegerLiteral
   */
  private int parseNumericLiteral() {

    return parseIntegerLiteral();

  }

  /**
   * Parses the the rule VarRef according to the following production rule:
   * <p>
   * [44] VarRef ::= "$" VarName .
   * </p>
   */
  private void parseVarRef() {

    consume(TokenType.DOLLAR, true);
    final String varName = parseVarName();
    mPipeBuilder.addVarRefExpr(getTransaction(), varName);
  }

  /**
   * Parses the the rule PatenthesizedExpr according to the following
   * production rule:
   * <p>
   * [45] ParenthesizedExpr ::= "(" Expr? ")" .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseParenthesizedExpr() throws TTXPathException {

    consume(TokenType.OPEN_BR, true);
    if (!(mToken.getType() == TokenType.CLOSE_BR)) {
      parseExpression();
    }
    consume(TokenType.CLOSE_BR, true);

  }

  /**
   * Parses the the rule ContextItemExpr according to the following production
   * rule:
   * <p>
   * [46] ContextItemExpr ::= "." .
   * </p>
   */
  private void parseContextItemExpr() {

    consume(TokenType.POINT, true);

    mPipeBuilder.addStep(new SelfAxis(getTransaction()));
  }

  /**
   * Parses the the rule FunctionCall according to the following production
   * rule:
   * <p>
   * [47] FunctionCall ::= < QName "(" > (ExprSingle ("," ExprSingle)*)? ")" .
   * </p>
   * 
   * @throws TTXPathException
   */
  private void parseFunctionCall() throws TTXPathException {

    final String funcName = parseQName();

    consume(TokenType.OPEN_BR, true);

    int num = 0;
    if (!(mToken.getType() == TokenType.CLOSE_BR)) {

      do {

        parseExprSingle();
        num++;

      } while (is(TokenType.COMMA, true));
    }

    consume(TokenType.CLOSE_BR, true);
    mPipeBuilder.addFunction(getTransaction(), funcName, num);

  }

  /**
   * Parses the the rule SingleType according to the following production
   * rule:
   * <p>
   * [48] SingleType ::= AtomicType "?"? .
   * </p>
   * 
   * @return SingleType
   * @throws TTXPathException
   */
  private SingleType parseSingleType() throws TTXPathException {

    final String atomicType = parseAtomicType();
    final boolean intero = is(TokenType.INTERROGATION, true);
    return new SingleType(atomicType, intero);
  }

  /**
   * Parses the the rule SequenceType according to the following production
   * rule: [
   * <p>
   * 49] SequenceType ::= (ItemType OccurrenceIndicator?) | <"void" "(" ")"> .
   * </p>
   * 
   * @return SequenceType
   */
  private SequenceType parseSequenceType() {

    if (is("empty-sequence", true)) {
      consume(TokenType.OPEN_BR, true);
      consume(TokenType.CLOSE_BR, true);
      return new SequenceType();

    } else {
      final IFilter filter = parseItemType();
      if (isWildcard()) {
        final char wildcard = parseOccuranceIndicator();
        return new SequenceType(filter, wildcard);
      }
      return new SequenceType(filter);
    }
  }

  /**
   * @return true, if the current token is a '?', a '*' or a '+'.
   */
  private boolean isWildcard() {

    final TokenType type = mToken.getType();
    return (type == TokenType.STAR || type == TokenType.PLUS || type == TokenType.INTERROGATION);
  }

  /**
   * Parses the the rule OccuranceIndicator according to the following
   * production rule:
   * <p>
   * [50] OccurrenceIndicator ::= "?" | "*" | "+" .
   * </p>
   * 
   * @return wildcard
   */
  private char parseOccuranceIndicator() {

    char wildcard;

    if (is(TokenType.STAR, true)) {
      wildcard = '*';
    } else if (is(TokenType.PLUS, true)) {
      wildcard = '+';
    } else {
      consume(TokenType.INTERROGATION, true);
      wildcard = '?';

    }
    return wildcard;

  }

  /**
   * Parses the the rule ItemType according to the following production rule:
   * <p>
   * [51] ItemType ::= AtomicType | KindTest | <"item" "(" ")"> .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseItemType() {

    IFilter filter;
    if (isKindTest()) {
      filter = parseKindTest();
    } else if (is("item", true)) {
      consume(TokenType.OPEN_BR, true);
      consume(TokenType.CLOSE_BR, true);

      filter = new ItemFilter(getTransaction());
    } else {
      final String atomic = parseAtomicType();
      filter = new TypeFilter(getTransaction(), atomic);
    }
    return filter;
  }

  /**
   * Parses the the rule AtomicTypr according to the following production
   * rule:
   * <p>
   * [52] AtomicType ::= QName .
   * </p>
   * 
   * @return parseQName
   */
  private String parseAtomicType() {

    return parseQName();
  }

  /**
   * Parses the the rule KindTest according to the following production rule:
   * <p>
   * [53] KindTest ::= DocumentCreater | ElementTest | AttributeTest | SchemaElementTest | SchemaAttributeTest
   * | PITest | CommentTest | TextTest | AnyKindTest .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseKindTest() {

    IFilter filter;
    final String test = mToken.getContent();

    if ("document-node".equals(test)) {
      filter = parseDocumentTest();
    } else if ("element".equals(test)) {
      filter = parseElementTest();
    } else if ("attribute".equals(test)) {
      filter = parseAttributeTest();
    } else if ("schema-element".equals(test)) {
      filter = parseSchemaElementTest();
    } else if ("schema-attribute".equals(test)) {
      filter = parseSchemaAttributeTest();
    } else if ("processing-instruction".equals(test)) {
      filter = parsePITest();
    } else if ("comment".equals(test)) {
      filter = parseCommentTest();
    } else if ("text".equals(test)) {
      filter = parseTextTest();
    } else {
      filter = parseAnyKindTest();
    }
    return filter;
  }

  /**
   * Parses the the rule AnyKindTest according to the following production
   * rule:
   * <p>
   * [54] AnyKindTest ::= <"node" "("> ")" .
   * <p>
   * 
   * @return NodeFilter
   */
  private IFilter parseAnyKindTest() {

    consume("node", true);
    consume(TokenType.OPEN_BR, true);
    consume(TokenType.CLOSE_BR, true);

    return new NodeFilter(getTransaction());
  }

  /**
   * Checks if a given token represents a kind test.
   * 
   * @return true, if the token is a kind test
   */
  private boolean isKindTest() {

    final String content = mToken.getContent();
    // lookahead is necessary, in a of e.g. a node filter, that filters
    // nodes
    // with a name like text:bla or node:bla, where text and node are the
    // namespace prefixes.
    return (("node".equals(content) || "attribute".equals(content) || "schema-attribute".equals(content)
      || "schema-element".equals(content) || "element".equals(content) || "text".equals(content)
      || "comment".equals(content) || "document-node".equals(content) || "processing-instruction"
        .equals(content)) && mScanner.lookUpTokens(1).getType() == TokenType.OPEN_BR);
  }

  /**
   * Parses the the rule DocumentCreater according to the following production
   * rule:
   * <p>
   * [55] DocumentCreater ::= <"document-node" "("> (ElementTest | SchemaElementTest)? ")" .
   * <p>
   * 
   * @return filter
   */
  private IFilter parseDocumentTest() {

    consume("document-node", true);
    consume(TokenType.OPEN_BR, true);
    IFilter filter = new DocumentRootNodeFilter(getTransaction());

    IFilter innerFilter;
    if (mToken.getContent().equals("element")) {
      innerFilter = parseElementTest();
      filter = new NestedFilter(getTransaction(), filter, innerFilter);
    } else if (mToken.getContent().equals("schema-element")) {
      innerFilter = parseSchemaElementTest();
      filter = new NestedFilter(getTransaction(), filter, innerFilter);
    }

    consume(TokenType.CLOSE_BR, true);

    return filter;
  }

  /**
   * Parses the the rule TextTest according to the following production rule:
   * <p>
   * [56] TextTest ::= <"text" "("> ")" .
   * </p>
   * 
   * @return TextFilter
   */
  private IFilter parseTextTest() {

    consume("text", true);
    consume(TokenType.OPEN_BR, true);
    consume(TokenType.CLOSE_BR, true);

    return new TextFilter(getTransaction());
  }

  /**
   * Parses the the rule CommentTest according to the following production
   * rule:
   * <p>
   * [57] CommentTest ::= <"comment" "("> ")" .
   * </p>
   * 
   * @return CommonFilter
   */
  private IFilter parseCommentTest() {

    consume("comment", true);
    consume(TokenType.OPEN_BR, true);
    consume(TokenType.CLOSE_BR, true);

    return new CommentFilter(getTransaction());
  }

  /**
   * Parses the the rule PITest according to the following production rule:
   * <p>
   * [58] PITest ::= <"processing-instruction" "("> (NCName | StringLiteral)? ")" .
   * </p>
   * 
   * @return filter
   */
  private IFilter parsePITest() {

    consume("processing-instruction", true);
    consume(TokenType.OPEN_BR, true);

    IFilter filter = new PIFilter(getTransaction());

    if (!is(TokenType.CLOSE_BR, true)) {
      String stringLiteral;
      if (isQuote()) {
        final byte[] param =
          ((IValNode)getTransaction().getItemList().getItem(parseStringLiteral()).get()).getRawValue();
        stringLiteral = Arrays.toString(param);
      } else {
        stringLiteral = parseNCName();
      }

      consume(TokenType.CLOSE_BR, true);

      filter = new NestedFilter(getTransaction(), filter, new NameFilter(getTransaction(), stringLiteral));
    }

    return filter;
  }

  /**
   * @return true, if token is a ' or a "
   */
  private boolean isQuote() {

    final TokenType type = mToken.getType();
    return (type == TokenType.SINGLE_QUOTE || type == TokenType.DBL_QUOTE);
  }

  /**
   * Parses the the rule AttributeTest according to the following production
   * rule:
   * <p>
   * [59] AttributeTest ::= <"attribute" "("> (AttribNameOrWildcard ("," TypeName)?)? ")" .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseAttributeTest() {

    consume("attribute", true);
    consume(TokenType.OPEN_BR, true);

    IFilter filter = new AttributeFilter(getTransaction());

    if (!(mToken.getType() == TokenType.CLOSE_BR)) {
      // add name filter
      final String name = parseAttributeNameOrWildcard();
      if (!name.equals("*")) {
        filter = new NestedFilter(getTransaction(), filter, new NameFilter(getTransaction(), name));
      } // if it is '*', all attributes are accepted, so the normal
        // attribute
        // filter is sufficient

      if (is(TokenType.COMMA, true)) {
        // add type filter
        filter =
          new NestedFilter(getTransaction(), filter, new TypeFilter(getTransaction(), parseTypeName()));
      }
    }

    consume(TokenType.CLOSE_BR, true);

    return filter;
  }

  /**
   * Parses the the rule AttributeOrWildcard according to the following
   * production rule:
   * <p>
   * [60] AttribNameOrWildcard ::= AttributeName | "*" .
   * </p>
   * 
   * @return name
   */
  private String parseAttributeNameOrWildcard() {

    String name;

    if (is(TokenType.STAR, true)) {
      name = mToken.getContent();
    } else {
      name = parseAttributeName();
    }

    return name;
  }

  /**
   * Parses the the rule SchemaAttributeTest according to the following
   * production rule:
   * <p>
   * [61] SchemaAttributeTest ::= <"schema-attribute" "("> AttributeDeclaration ")" .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseSchemaAttributeTest() {

    consume("schema-attribute", true);
    consume(TokenType.OPEN_BR, true);

    final IFilter filter = new SchemaAttributeFilter(getTransaction()/*
                                                                      * ,
                                                                      * parseAttributeDeclaration
                                                                      * ()
                                                                      */);

    consume(TokenType.CLOSE_BR, true);

    return filter;
  }

  // /**
  // * Parses the the rule AttributeDeclaration according to the following
  // * production rule:
  // * <p>
  // * [62] AttributeDeclaration ::= AttributeName .
  // * </p>
  // */
  // private String parseAttributeDeclaration() {
  //
  // return parseAttributeName();
  // }

  /**
   * Parses the the rule ElementTest according to the following production
   * rule:
   * <p>
   * [63] ElementTest ::= <"element" "("> (ElementNameOrWildcard ("," TypeName "?"?)?)? ")" .
   * </p>
   * 
   * @return filter
   */
  private IFilter parseElementTest() {

    consume("element", true);
    consume(TokenType.OPEN_BR, true);

    IFilter filter = new ElementFilter(getTransaction());

    if (!(mToken.getType() == TokenType.CLOSE_BR)) {

      final String mName = parseElementNameOrWildcard();
      if (!mName.equals("*")) {
        filter = new NestedFilter(getTransaction(), filter, new NameFilter(getTransaction(), mName));
      } // if it is '*', all elements are accepted, so the normal element
        // filter is sufficient

      if (is(TokenType.COMMA, true)) {

        filter =
          new NestedFilter(getTransaction(), filter, new TypeFilter(getTransaction(), parseTypeName()));

        if (is(TokenType.INTERROGATION, true)) {
          // TODO: Nilled property of node can be true or false.
          // Without, must
          // be false
          throw new NoSuchElementException("'?' is not supported yet.");
        }
      }
    }

    consume(TokenType.CLOSE_BR, true);

    return filter;
  }

  /**
   * Parses the the rule ElementNameOrWildcard according to the following
   * production rule:
   * <p>
   * [64] ElementNameOrWildcard ::= ElementName | "*" .
   * </p>
   * 
   * @return name
   */
  private String parseElementNameOrWildcard() {

    String name;

    if (is(TokenType.STAR, true)) {
      name = mToken.getContent();
    } else {
      name = parseElementName();
    }

    return name;
  }

  /**
   * Parses the the rule SchemaElementTest according to the following
   * production rule:
   * <p>
   * [65] SchemaElementTest ::= <"schema-element" "("> ElementDeclaration ")" .
   * </p>
   * 
   * @return SchemaElementFilter
   */
  private IFilter parseSchemaElementTest() {

    consume("schema-element", true);
    consume(TokenType.OPEN_BR, true);

    // final String elementDec = parseElementDeclaration();

    consume(TokenType.CLOSE_BR, true);

    return new SchemaElementFilter(getTransaction()/* ,elementDec */);
  }

  // /**
  // * Parses the the rule ElementDeclaration according to the following
  // * production rule:
  // * <p>
  // * [66] ElementDeclaration ::= ElementName .
  // * </p>
  // */
  // private String parseElementDeclaration() {
  //
  // return parseElementName();
  // }

  /**
   * Parses the the rule AttributeName according to the following production
   * rule:
   * <p>
   * [67] AttributeName ::= QName .
   * </p>
   * 
   * @return parseQName
   */
  private String parseAttributeName() {

    return parseQName();
  }

  /**
   * Parses the the rule ElementName according to the following production
   * rule:
   * <p>
   * [68] ElementName ::= QName .
   * </p>
   * 
   * @return parseQName
   */
  private String parseElementName() {

    return parseQName();
  }

  /**
   * Parses the the rule TypeName according to the following production rule:
   * <p>
   * [69] TypeName ::= QName .
   * </p>
   * 
   * @return parseQName
   */
  private String parseTypeName() {

    return parseQName();
  }

  /**
   * Parses the the rule IntegerLiteral according to the following production
   * rule:
   * <p>
   * [70] IntegerLiteral ::= Digits => IntergerLiteral : Token.Value .
   * </p>
   * 
   * @return parseItem
   */
  private int parseIntegerLiteral() {
    String value = mToken.getContent();
    String type = "xs:integer";

    if (is(TokenType.VALUE, false)) {

      // is at least decimal literal (could also be a double literal)
      if (mToken.getType() == TokenType.POINT) {
        // TODO: not so nice, try to find a better solution
        final boolean isDouble = mScanner.lookUpTokens(2).getType() == TokenType.E_NUMBER;
        value = parseDecimalLiteral(value);
        type = isDouble ? "xs:double" : "xs:decimal";
      }

      // values containing an 'e' are double literals
      if (mToken.getType() == TokenType.E_NUMBER) {
        value = parseDoubleLiteral(value);
        type = "xs:double";
      }

    } else {
      // decimal literal that starts with a "."
      final boolean isDouble = mScanner.lookUpTokens(2).getType() == TokenType.E_NUMBER;
      value = parseDecimalLiteral("");
      type = isDouble ? "xs:double" : "xs:decimal";
    }

    is(TokenType.SPACE, true);

    final INode intLiteral = new AtomicValue(TypedValue.getBytes(value), getTransaction().keyForName(type));
    return getTransaction().getItemList().addItem(intLiteral);
  }

  /**
   * Parses the the rule DecimalLiteral according to the following production
   * rule:
   * <p>
   * [71] DecimalLiteral ::= ("." Digits) | (Digits "." [0-9]*) => DecimalLiteral : ("." Token.VALUE) |
   * (Token.VALUE "." Token.VALUE?) .
   * </p>
   * 
   * @param mValue
   *          Value to Parse
   * @return dValue
   */
  private String parseDecimalLiteral(final String mValue) {

    consume(TokenType.POINT, false);
    String dValue = mValue;

    if (mToken.getType() == TokenType.VALUE) {
      dValue = mValue + "." + mToken.getContent();
      consume(TokenType.VALUE, false);

      if (mToken.getType() == TokenType.E_NUMBER) {
        // TODO: set type to double
        dValue = parseDoubleLiteral(dValue);
      }
    }

    return dValue;
  }

  /**
   * Parses the the rule DoubleLiteral according to the following production
   * rule:
   * <p>
   * [72] DoubleLiteral ::= (("." Digits) | (Digits ("." [0-9]*)?)) [eE] [+-]? Digits . DoubleLiteral : (("."
   * Token.VALUE) | (Token.VALUE ("." Token.VALUE?)?)) ("e" | "E") [+-]? Token.VALUE .
   * </p>
   * 
   * @param mValue
   *          Value to Parse
   * @return dValue
   * 
   */
  private String parseDoubleLiteral(final String mValue) {

    final StringBuilder dValue = new StringBuilder(mValue);

    if (is(TokenType.E_NUMBER, false)) {
      dValue.append("E");
      dValue.append(mToken.getContent());
    }

    if (is(TokenType.PLUS, false) || is(TokenType.MINUS, false)) {
      dValue.append(mToken.getContent());
    }

    consume(TokenType.VALUE, true);

    return dValue.toString();
  }

  /**
   * Parses the the rule StringLiteral according to the following production
   * rule:
   * <p>
   * [73] StringLiteral ::= ('"' (('"' '"') | [^"])* '"') | ("'" (("'" "'") | [^'])* "'" .
   * </p>
   * 
   * @return parseStringLiteral
   */
  private int parseStringLiteral() {

    final StringBuilder mValue = new StringBuilder();

    if (is(TokenType.DBL_QUOTE, true)) {

      do {

        while (mToken.getType() != TokenType.DBL_QUOTE) {
          mValue.append(mToken.getContent());
          // consume(Token.TEXT, false); // TODO: does not need to be
          // a text
          // could also be a value
          mToken = mScanner.nextToken();
        }

        consume(TokenType.DBL_QUOTE, true);
      } while (is(TokenType.DBL_QUOTE, true));

    } else {

      consume(TokenType.SINGLE_QUOTE, true);

      do {

        while (mToken.getType() != TokenType.SINGLE_QUOTE) {
          mValue.append(mToken.getContent());
          // consume(Token.TEXT, false);
          mToken = mScanner.nextToken();
        }

        consume(TokenType.SINGLE_QUOTE, true);
      } while (is(TokenType.SINGLE_QUOTE, true));

    }

    final INode mStringLiteral =
      new AtomicValue(TypedValue.getBytes(mValue.toString()), getTransaction().keyForName("xs:string"));
    return (getTransaction().getItemList().addItem(mStringLiteral));
  }

  /**
   * Parses the the rule VarName according to the following production rule:
   * <p>
   * [74] VarName ::= QName .
   * </p>
   * 
   * @return string representation of variable name.
   */
  private String parseVarName() {

    return parseQName();
  }

  /**
   * Specifies whether the current token is a multiplication operator.
   * Multiplication operators are: '*', 'div�, 'idiv' and 'mod'.
   * 
   * @return true, if current token is a multiplication operator
   */
  private boolean isMultiplication() {

    return (is(TokenType.STAR, true) || is("div", true) || is("idiv", true) || is("mod", true));

  }

  /**
   * Parses the the rule QName according to the following production rule:
   * http://www.w3.org/TR/REC-xml-names
   * <p>
   * [7] QName ::= PrefixedName | UnprefixedName
   * </p>
   * <p>
   * [8] PrefixedName ::= Prefix ':' LocalPart
   * </p>
   * <p>
   * [9] UnprefixedName ::= LocalPart
   * </p>
   * <p>
   * [10] Prefix ::= NCName
   * </p>
   * <p>
   * [11] LocalPart ::= NCName => QName ::= (NCName ":" )? NCName .
   * </p>
   * 
   * @return string representation of QName
   */
  private String parseQName() {

    String qName = parseNCName(); // can be prefix or localPartName

    if (is(TokenType.COLON, false)) {

      qName += ":";
      qName += parseNCName(); // is localPartName
    }

    return qName;
  }

  /**
   * Parses the the rule NCName according to the following production rule:
   * http://www.w3.org/TR/REC-xml-names
   * <p>
   * [4] NCName ::= NCNameStartChar NCNameChar*
   * </p>
   * <p>
   * [5] NCNameChar ::= NameChar - ':'
   * </p>
   * <p>
   * [6] NCNameStartChar ::= Letter | '_' => NCName : Token.TEXT .
   * </p>
   * 
   * @return string representation of NCName
   */
  private String parseNCName() {

    final String ncName = mToken.getContent();

    consume(TokenType.TEXT, true);

    return ncName;
  }

  /**
   * Consumes a token. Tests if it really has the expected type and if not
   * returns an error message. Otherwise gets a new token from the scanner. If
   * that new token is of type whitespace and the ignoreWhitespace parameter
   * is true, a new token is retrieved, until the current token is not of type
   * whitespace.
   * 
   * @param mType
   *          the specified token type
   * @param mIgnoreWhitespace
   *          if true all new tokens with type whitespace are ignored and
   *          the next token is retrieved from the scanner
   */
  private void consume(final TokenType mType, final boolean mIgnoreWhitespace) {

    if (!is(mType, mIgnoreWhitespace)) {
      // error found by parser - stopping
      throw new IllegalStateException("Wrong token after " + mScanner.begin() + " at position "
        + mScanner.getPos() + " found " + mToken.getType() + " expected " + mType + ".");
    }
  }

  /**
   * Consumes a token. Tests if it really has the expected name and if not
   * returns an error message. Otherwise gets a new token from the scanner. If
   * that new token is of type whitespace and the ignoreWhitespace parameter
   * is true, a new token is retrieved, until the current token is not of type
   * whitespace.
   * 
   * @param mName
   *          the specified token content
   * @param mIgnoreWhitespace
   *          if true all new tokens with type whitespace are ignored and
   *          the next token is retrieved from the scanner
   */
  private void consume(final String mName, final boolean mIgnoreWhitespace) {

    if (!is(mName, mIgnoreWhitespace)) {
      // error found by parser - stopping
      throw new IllegalStateException("Wrong token after " + mScanner.begin() + " found "
        + mToken.getContent() + ". Expected " + mName);
    }
  }

  /**
   * Returns true or false if a token has the expected name. If the token has
   * the given name, it gets a new token from the scanner. If that new token
   * is of type whitespace and the ignoreWhitespace parameter is true, a new
   * token is retrieved, until the current token is not of type whitespace.
   * 
   * @param mName
   *          the specified token content
   * @param mIgnoreWhitespace
   *          if true all new tokens with type whitespace are ignored and
   *          the next token is retrieved from the scanner
   * @return is
   */
  private boolean is(final String mName, final boolean mIgnoreWhitespace) {

    if (!mName.equals(mToken.getContent())) {
      return false;
    }

    if (mToken.getType() == TokenType.COMP || mToken.getType() == TokenType.EQ
      || mToken.getType() == TokenType.N_EQ || mToken.getType() == TokenType.PLUS
      || mToken.getType() == TokenType.MINUS || mToken.getType() == TokenType.STAR) {
      return is(mToken.getType(), mIgnoreWhitespace);
    } else {
      return is(TokenType.TEXT, mIgnoreWhitespace);
    }
  }

  /**
   * Returns true or false if a token has the expected type. If so, a new
   * token is retrieved from the scanner. If that new token is of type
   * whitespace and the ignoreWhitespace parameter is true, a new token is
   * retrieved, until the current token is not of type whitespace.
   * 
   * @param mType
   *          the specified token content
   * @param mIgnoreWhitespace
   *          if true all new tokens with type whitespace are ignored and
   *          the next token is retrieved from the scanner
   * @return is
   */
  private boolean is(final TokenType mType, final boolean mIgnoreWhitespace) {
    if (mType != mToken.getType()) {
      return false;
    }

    // scan next token
    mToken = mScanner.nextToken();

    if (mIgnoreWhitespace) {
      while (mToken.getType() == TokenType.SPACE) {
        // scan next token
        mToken = mScanner.nextToken();
      }
    }

    return true;
  }

  /**
   * Returns a queue containing all pipelines (chains of nested axis and
   * filters) to execute the query.
   * 
   * @return the query pipelines
   */
  public IAxis getQueryPipeline() {
    return mPipeBuilder.getPipeline();
  }

  /**
   * Returns the read transaction.
   * 
   * @return the current transaction
   */
  private INodeReadTrx getTransaction() {
    return mRTX;
  }
}
