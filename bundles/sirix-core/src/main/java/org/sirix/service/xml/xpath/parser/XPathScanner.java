/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.service.xml.xpath.parser;

/**
 * <h1>XPathScanner</h1>
 * <p>
 * Lexical scanner to extract tokens from the query.
 * </p>
 * <p>
 * This scanner is used to interpret the query. It reads the the query string char by char and
 * specifies the type of the input and creates a token for every logic text unit.
 * </p>
 */
public final class XPathScanner {

  /** The XPath query to scan. */
  private final String mQuery;

  /** The current position of the cursor to the query string. */
  private int mPos;

  /** Start position of the last item. */
  private int mLastPos;

  /** Scanner states. */
  private enum State {
    /** Start state. */
    START,
    /** Number state. */
    NUMBER,
    /** Text state. */
    TEXT,
    /** Special state. */
    SPECIAL,
    /** Special state with 2 caracters. */
    SPECIAL2,
    /** Comment state. */
    COMMENT,
    /** Enum state. */
    E_NUM,
    /** Unknown state. */
    UNKNOWN
  }

  /** The state the scanner is currently in. */
  private State mState;

  /** Contains the current content of the token. */
  private StringBuilder mOutput;

  /**
   * Defines if all digits of a token have been read or if the token still can have more digits.
   */
  private boolean mFinnished;

  /** The type of the current token. */
  private TokenType mType;

  /** The current character. */
  private char mInput;

  /**
   * State with which the next token starts. Sometimes it is not needed to start in the start state
   * for some tokens, as their type is known because of the preceding token.
   */
  private State mStartState;

  /**
   * Counts the number of nested comments. Is needed to distinguish whether the current token is
   * part of a comment, or part of the query. If mCommentCount > 0, the current token is part of a
   * comment.
   */
  private int mCommentCount;

  /**
   * Constructor. Initializes the internal state. Receives query and adds a end mark to it.
   * 
   * @param paramQuery the query to scan
   */
  public XPathScanner(final String paramQuery) {
    mQuery = paramQuery + '#'; // end mark to recognize the end
    mPos = 0;
    mLastPos = mPos;
    mStartState = State.START;
    mCommentCount = 0;
  }

  /**
   * Reads the string char by char and returns one token by call. The scanning starts in the start
   * state, if not further specified before, and specifies the next scanner state and the type of
   * the future token according to its first char. As soon as the current char does not fit the
   * conditions for the current token type, the token is generated and returned.
   * 
   * @return token the new token
   */
  public XPathToken nextToken() {
    // some tokens start in another state than the START state
    mState = mStartState;
    // reset startState
    mStartState = State.START;
    mOutput = new StringBuilder();
    mFinnished = false;
    mType = TokenType.INVALID;
    mLastPos = mPos;

    do {
      mInput = mQuery.charAt(mPos);

      switch (mState) {
        case START: // specify token type according to first char
          scanStart();
          break;
        case NUMBER: // number
          scanNumber();
          break;
        case TEXT: // some text, could be a name
          scanText();
          break;
        case SPECIAL2: // special character that could have 2 digits
          scanTwoDigitSpecial();
          break;
        case COMMENT:
          scanComment();
          break;
        case E_NUM:
          scanENum();
          break;
        default:
          mPos++;
          mFinnished = true;
      }
    } while (!mFinnished || mPos >= mQuery.length());

    if (mCommentCount > 0) {
      throw new IllegalStateException("Error in Query. Comment does not end.");
    }

    return new VariableXPathToken(mOutput.toString(), mType);
  }

  /**
   * Scans the first character of a token and decides, what type it is.
   */
  private void scanStart() {
    if (isNumber(mInput)) {
      mState = State.NUMBER;
      mOutput.append(mInput);
      mType = TokenType.VALUE; // number
    } else if (isFirstLetter(mInput)) {
      mState = State.TEXT; // word
      mOutput.append(mInput);
      mType = TokenType.TEXT;
    } else if (isSpecial(mInput)) {
      mState = State.SPECIAL; // special character with only one digit
      mOutput.append(mInput);
      mType = retrieveType(mInput);
      mFinnished = true;
    } else if (isTwoDigistSpecial(mInput)) {
      mState = State.SPECIAL2; // 2 digit special character
      mOutput.append(mInput);
      mType = retrieveType(mInput);
    } else if ((mInput == ' ') || (mInput == '\n')) {
      mState = State.START;
      mOutput.append(mInput);
      mFinnished = true;
      mType = TokenType.SPACE;
    } else if (mInput == '#') {
      mType = TokenType.END; // end of query
      mFinnished = true;
      mPos--;
    } else {
      mState = State.UNKNOWN; // unknown character
      mOutput.append(mInput);
      mFinnished = true;
    }
    mPos++;
  }

  /**
   * Returns the type of the given character.
   * 
   * @param paramInput the character the type should be determined
   * @return type of the given character
   */
  private TokenType retrieveType(final char paramInput) {
    TokenType type;
    switch (paramInput) {
      case ',':
        type = TokenType.COMMA;
        break;
      case '(':
        type = TokenType.OPEN_BR;
        break;
      case ')':
        type = TokenType.CLOSE_BR;
        break;
      case '[':
        type = TokenType.OPEN_SQP;
        break;
      case ']':
        type = TokenType.CLOSE_SQP;
        break;
      case '@':
        type = TokenType.AT;
        break;
      case '=':
        type = TokenType.EQ;
        break;
      case '<':
      case '>':
        type = TokenType.COMP;
        break;
      case '!':
        type = TokenType.N_EQ;
        break;
      case '/':
        type = TokenType.SLASH;
        break;
      case ':':
        type = TokenType.COLON;
        break;
      case '.':
        type = TokenType.POINT;
        break;
      case '+':
        type = TokenType.PLUS;
        break;
      case '-':
        type = TokenType.MINUS;
        break;
      case '\'':
        type = TokenType.SINGLE_QUOTE;
        break;
      case '"':
        type = TokenType.DBL_QUOTE;
        break;
      case '$':
        type = TokenType.DOLLAR;
        break;
      case '?':
        type = TokenType.INTERROGATION;
        break;
      case '*':
        type = TokenType.STAR;
        break;
      case '|':
        type = TokenType.OR;
        break;
      default:
        type = TokenType.INVALID;
    }
    return type;
  }

  /**
   * Checks if the given character is a valid first letter.
   * 
   * @param paramInput The character to check.
   * @return Returns true, if the character is a first letter.
   */
  private boolean isFirstLetter(final char paramInput) {
    return (paramInput >= 'a' && paramInput <= 'z') || (paramInput >= 'A' && paramInput <= 'Z')
        || (paramInput == '_');
  }

  /**
   * Checks if the given character is a number.
   * 
   * @param paramInput The character to check.
   * @return Returns true, if the character is a number.
   */
  private boolean isNumber(final char paramInput) {
    return paramInput >= '0' && paramInput <= '9';
  }

  /**
   * Checks if the given character is a special character that can have 2 digits.
   * 
   * @param paramInput The character to check.
   * @return Returns true, if the character is a special character that can have 2 digits.
   */
  private boolean isTwoDigistSpecial(final char paramInput) {
    return (paramInput == '<') || (paramInput == '>') || (paramInput == '(') || (paramInput == '!')
        || (paramInput == '/') || (paramInput == '.');
  }

  /**
   * Checks if the given character is a special character.
   * 
   * @param paramInput The character to check.
   * @return Returns true, if the character is a special character.
   */
  private boolean isSpecial(final char paramInput) {
    return (paramInput == ')') || (paramInput == ';') || (paramInput == ',') || (paramInput == '@')
        || (paramInput == '[') || (paramInput == ']') || (paramInput == '=') || (paramInput == '"')
        || (paramInput == '\'') || (paramInput == '$') || (paramInput == ':') || (paramInput == '|')
        || (paramInput == '+') || (paramInput == '-') || (paramInput == '?') || (paramInput == '*');
  }

  /**
   * Scans a number token. A number only consists of digits.
   */
  private void scanNumber() {
    if (mInput >= '0' && mInput <= '9') {
      mOutput.append(mInput);
      mPos++;
    } else {
      // could be an e-number
      if (mInput == 'E' || mInput == 'e') {
        mStartState = State.E_NUM;
      }
      mFinnished = true;
    }
  }

  /**
   * Scans text token. A text is everything that with a character. It can contain numbers, all
   * letters in upper or lower case and underscores.
   */
  private void scanText() {
    if (isLetter(mInput)) {
      mOutput.append(mInput);
      mPos++;

    } else {
      mType = TokenType.TEXT;
      mFinnished = true;
    }
  }

  /**
   * Scans special characters that can have more then one digit. E.g. ==, !=, <=, >=, //, .., (:
   */
  private void scanTwoDigitSpecial() {
    if (mInput == '='
        && (mType == TokenType.COMP || mType == TokenType.EQ || mType == TokenType.N_EQ)) {
      mOutput.append(mInput);
      mPos++;
    } else if (mInput == '/' && (mType == TokenType.SLASH)) {
      mOutput.append(mInput);
      mType = TokenType.DESC_STEP;
      mPos++;
    } else if (mInput == '.' && (mType == TokenType.POINT)) {
      mOutput.append(mInput);
      mType = TokenType.PARENT;
      mPos++;
    } else if (mInput == '<' && mOutput.toString().equals("<")) {
      mOutput.append(mInput);
      mType = TokenType.L_SHIFT;
      mPos++;
    } else if (mInput == '>' && mOutput.toString().equals(">")) {
      mOutput.append(mInput);
      mType = TokenType.R_SHIFT;
      mPos++;
    } else if (mInput == ':' && mType == TokenType.OPEN_BR) {
      // could be start of a comment
      mOutput = new StringBuilder();
      mType = TokenType.COMMENT;
      mCommentCount++;
      mState = State.COMMENT;
      mPos++;
    } else {
      mFinnished = true;
    }
  }

  /**
   * Scans all numbers that contain an e.
   */
  private void scanENum() {
    if (mInput == 'E' || mInput == 'e') {
      mOutput.append(mInput);
      mState = State.START;
      mType = TokenType.E_NUMBER;
      mFinnished = true;
      mPos++;
    } else {
      mFinnished = true;
      mState = State.START;
      mType = TokenType.INVALID;
    }
  }

  /**
   * Scans comments.
   */
  private void scanComment() {
    final char input = mQuery.charAt(mPos + 1);
    if (mInput == ':') {
      // check if is end of comment, indicated by ':)'
      if (input == ')') {
        mCommentCount--;
        if (mCommentCount == 0) {
          mState = State.START;
          // increment position, because next digit has already been
          // processed
          mPos++;
        }

      }
    } else if (mInput == '(') {
      // check if start of new nested comment, indicated by '(:'
      if (input == ':') {
        mCommentCount++;
      }
    }
    mPos++;
  }

  /**
   * Checks if the given character is a letter.
   * 
   * @param paramInput the character to check
   * @return returns true, if the character is a letter
   */
  private boolean isLetter(final char paramInput) {
    return (paramInput >= '0' && paramInput <= '9') || (paramInput >= 'a' && paramInput <= 'z')
        || (paramInput >= 'A' && paramInput <= 'Z') || (paramInput == '_') || (paramInput == '-')
        || (paramInput == '.');
  }

  /**
   * Return the token that will be returned by the scanner after the call of nextToken(), without
   * changing the internal state of the scanner.
   * 
   * @param paramNext number of next tokens to be read
   * @return token that will be read after calling nextToken()
   */
  public XPathToken lookUpTokens(final int paramNext) {
    int nextCount = paramNext;

    // save current position of the scanner, to restore it later
    final int lastPos = mPos;
    XPathToken token = nextToken();

    while (--nextCount > 0) {
      token = nextToken();
      if (token.getType() == TokenType.SPACE) {
        nextCount++;
      }
    }

    // reset position
    mPos = lastPos;
    return token;
  }

  /**
   * Returns the beginning of a query that has already been scanned. This can be used by the client
   * e.g. for error messages in case of unexpected token occurs.
   * 
   * @return string so far
   */
  public String begin() {
    return mQuery.substring(0, mLastPos);
  }

  /**
   * Return the current cursor position in the query.
   * 
   * @return current position of the cursor
   */
  public int getPos() {
    return mPos;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return mQuery;
  }
}
