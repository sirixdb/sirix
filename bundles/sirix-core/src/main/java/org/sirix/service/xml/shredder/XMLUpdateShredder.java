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

package org.sirix.service.xml.shredder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.Namespace;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.slf4j.LoggerFactory;
import org.sirix.access.Database;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.ISession;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTIOException;
import org.sirix.exception.TTUsageException;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.settings.EFixed;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.TypedValue;

/**
 * <h1>XMLUpdateShredder</h1>
 * 
 * <p>
 * Shredder, which updates a sirix revision to the next revision, as thus it just inserts or deletes nodes,
 * which have been changed. Renames are treated as insert new node, delete old node.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Deprecated
public final class XMLUpdateShredder extends XMLShredder implements Callable<Long> {

  /** {@link LogWrapper} reference. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(XMLUpdateShredder.class));

  /** File to parse. */
  protected transient File mFile;

  /** Events to parse. */
  protected transient Queue<XMLEvent> mEvents;

  /** Node key. */
  private transient long mNodeKey;

  /** Determines if a node is found in the sirix storage or not. */
  private transient boolean mFound;

  /** Determines if an end tag has been read while inserting nodes. */
  private transient boolean mInsertedEndTag;

  /** Determines if transaction has been moved to right sibling. */
  private transient boolean mMovedToRightSibling;

  /** Determines if node has been removed at the end of a subtree. */
  private transient boolean mRemovedNode;

  /**
   * Determines if it's a right sibling from the currently parsed node, where
   * the parsed node and the node in the sirix storage match.
   */
  private transient boolean mIsRightSibling;

  /** Last node key. */
  private transient long mLastNodeKey;

  /** Last node key in descendant check. */
  private transient long mLastDescCheckNodeKey;

  /**
   * The key of the node, when the nodes are equal if at all (used to check
   * right siblings and therefore if nodes have been deleted).
   */
  private transient long mKeyMatches;

  /** Maximum node key in revision. */
  private transient long mMaxNodeKey;

  /** Determines if changes should be commited. */
  private transient EShredderCommit mCommit;

  /** {@link XMLEventParser} used to check descendants. */
  private transient XMLEventReader mParser;

  /** Determines how many {@link XMLEvent}s currently have been parsed. */
  private transient long mElemsParsed;

  /** Level where the parser is in the file to shredder. */
  private transient int mLevelInToShredder;

  /** Level where the cursor is in the shreddered file. */
  private transient int mLevelInShreddered;

  /** {@link QName} of root node from which to shredder the subtree. */
  private transient QName mRootElem;

  // /** Determines if it's the last node or not. */
  // private transient boolean mIsLastNode;

  /** Determines where an insert in the tree occurs. */
  private enum EInternalInsert {
    /** Insert right after a start tag is parsed. */
    ATTOP,

    /** Insert at the start or at the middle of a subtree. */
    ATMIDDLEBOTTOM,

    /** Inserts have been made right before. */
    INTERMEDIATE,

    /** No insert occured. */
    NOINSERT
  }

  /** Determines where a delete in the tree occurs. */
  private transient EInternalInsert mInsert;

  /** Determines where a delete in the tree occurs. */
  private enum EDelete {
    /** Delete at the start or at the middle of a subtree. */
    ATSTARTMIDDLE,

    /** Delete right before an end tag is parsed. */
    ATBOTTOM,

    /** No delete occured. */
    NODELETE
  }

  /** Determines where a delete in the tree occured. */
  private transient EDelete mDelete;

  /** Cursor moved. */
  private enum EMoved {
    /** Cursor moved to first node. */
    FIRSTNODE,

    /** Cursor moved to first child or right sibling. */
    NOTTOPARENT,

    /** Cursor either didn't move or it moved to parent or right sibl. of parent. */
    TOPARENT,
  }

  /** Determines cursor movement in checkDescendants(StartElement). */
  private transient EMoved mMoved;

  /** Determines how to add a new node. */
  private enum EAdd {
    /** Add as first child. */
    ASFIRSTCHILD,

    /** Add as right sibling. */
    ASRIGHTSIBLING
  }

  /** Determines if a node has been inserted into sirix. */
  private transient boolean mInserted;

  /** Level in the checkDescendant(...) method. */
  private transient int mDescendantLevel;

  /** Determines if it's an empty element before an insert at the top of a subtree. */
  private transient boolean mEmptyElement;

  /**
   * Normal constructor to invoke a shredding process on a existing {@link INodeWriteTrx}.
   * 
   * @param paramWtx
   *          {@link INodeWriteTrx} where the new XML Fragment should be
   *          placed
   * @param paramReader
   *          {@link XMLEventReader} (StAX parser) of the XML Fragment
   * @param paramAddAsFirstChild
   *          if the insert is occuring on a node in an existing tree. <code>false</code> is not possible
   *          when wtx is on root node
   * @param paramData
   *          the data the update shredder operates on. Either a {@link List} of {@link XMLEvent}s or a
   *          {@link File}
   * @param paramCommit
   *          determines if changes should be commited
   * @throws TTUsageException
   *           if insertasfirstChild && updateOnly is both true OR if wtx is
   *           not pointing to doc-root and updateOnly= true
   * @throws TTIOException
   *           if sirix cannot access node keys
   * 
   */
  @SuppressWarnings("unchecked")
  public XMLUpdateShredder(final INodeWriteTrx paramWtx, final XMLEventReader paramReader,
    final EInsert paramAddAsFirstChild, final Object paramData, final EShredderCommit paramCommit)
    throws TTIOException {
    super(paramWtx, paramReader, paramAddAsFirstChild);
    if (paramData == null || paramCommit == null) {
      throw new IllegalArgumentException("None of the constructor parameters may be null!");
    }
    mMaxNodeKey = mWtx.getMaxNodeKey();
    mCommit = paramCommit;

    if (paramData instanceof File) {
      mFile = (File)paramData;
    } else if (paramData instanceof List<?>) {
      mEvents = (ArrayDeque<XMLEvent>)paramData;
    }
  }

  /**
   * Invoking the shredder.
   * 
   * @throws AbsTTException
   *           if sirix encounters something went wrong
   * @return revision of last revision (before commit)
   */
  @Override
  public Long call() throws AbsTTException {
    final long revision = mWtx.getRevisionNumber();
    updateOnly();
    if (mCommit == EShredderCommit.COMMIT) {
      mWtx.commit();
    }
    return revision;
  }

  /**
   * Update a shreddered file.
   * 
   * @throws AbsTTException
   *           if sirix encounters something went wrong
   */
  private void updateOnly() throws AbsTTException {
    try {
      // Initialize variables.
      mLevelInToShredder = 0;
      mLevelInShreddered = 0;
      mElemsParsed = 0;
      // mIsLastNode = false;
      mMovedToRightSibling = false;
      boolean firstEvent = true;

      // If structure already exists, make a sync against the current structure.
      if (mMaxNodeKey == 0) {
        // If no content is in the XML, a normal insertNewContent is executed.
        insertNewContent();
      } else {
        if (mWtx.getNode().getKind() == EKind.DOCUMENT_ROOT) {
          // Find the start key for the update operation.
          long startkey = EFixed.DOCUMENT_NODE_KEY.getStandardProperty() + 1;
          while (!mWtx.moveTo(startkey)) {
            startkey++;
          }
        }

        XMLEvent event = null;
        StringBuilder sBuilder = new StringBuilder();
        final XMLEventFactory fac = XMLEventFactory.newInstance();

        // Iterate over all nodes.
        while (mReader.hasNext()) {
          // Parsing the next event.
          if (mDelete == EDelete.ATSTARTMIDDLE) {
            /*
             * Do not move StAX parser forward if nodes have been deleted at the start or in the
             * middle of a subtree.
             */
            mDelete = EDelete.NODELETE;
          } else {
            // After an insert or after nodes were equal.
            event = mReader.nextEvent();
            if (event.isCharacters() && event.asCharacters().isWhiteSpace()) {
              continue;
            }
            mElemsParsed++;

            assert event != null;
            if (firstEvent) {
              // Setup start element from StAX parser.
              firstEvent = false;

              if (event.getEventType() == XMLStreamConstants.START_DOCUMENT) {
                while (mReader.hasNext() && event.getEventType() != XMLStreamConstants.START_ELEMENT) {
                  event = mReader.nextEvent();
                }
                assert event.getEventType() == XMLStreamConstants.START_ELEMENT;
                mElemsParsed++;
              }
              if (event.getEventType() != XMLStreamConstants.START_ELEMENT) {
                throw new IllegalStateException(
                  "StAX parser has to be on START_DOCUMENT or START_ELEMENT event!");
              }

              // Get root element of subtree or whole XML document to shredder.
              mRootElem = event.asStartElement().getName();
            } else if (event != null && event.isEndElement()
              && mRootElem.equals(event.asEndElement().getName()) && mLevelInToShredder == 1) {
              // End with shredding if end_elem equals root-elem.
              break;
            }
          }

          assert event != null;

          switch (event.getEventType()) {
          case XMLStreamConstants.START_ELEMENT:
            processStartTag(event.asStartElement());
            break;
          case XMLStreamConstants.CHARACTERS:
            sBuilder.append(event.asCharacters().getData());
            while (mReader.peek().getEventType() == XMLStreamConstants.CHARACTERS) {
              sBuilder.append(mReader.nextEvent().asCharacters().getData());
            }
            final Characters text = fac.createCharacters(sBuilder.toString().trim());
            processCharacters(text);
            sBuilder = new StringBuilder();
            break;
          case XMLStreamConstants.END_ELEMENT:
            processEndTag();
            break;
          default:
            // Other nodes which are currently not supported by sirix.
          }
        }

        // if (!mIsLastNode) {
        // if (mInserted) {
        // // Remove next node after node, which was inserted, because it must have been deleted.
        // if (mWtx.moveToRightSibling()) {
        // mWtx.remove();
        // }
        // } else {
        // // Remove current node (cursor has been moved to the next node already).
        // mWtx.remove();
        // }
        //
        // // Also remove any siblings.
        // boolean hasRightSibling = false;
        // while (mWtx.getStructuralNode().hasRightSibling()) {
        // hasRightSibling = true;
        // mWtx.remove();
        // }
        // if (hasRightSibling) {
        // mWtx.remove();
        // }
        // }

        mReader.close();
      }
      // TODO: use Java7 multi-catch feature.
    } catch (final XMLStreamException e) {
      throw new TTIOException(e);
    } catch (final IOException e) {
      throw new TTIOException(e);
    }

  }

  /**
   * Process start tag.
   * 
   * @param paramElem
   *          {@link StartElement} currently parsed.
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   * @throws IOException
   *           In case of any I/O error.
   * @throws AbsTTException
   *           In case of any sirix error.
   */
  private void processStartTag(final StartElement paramElem) throws IOException, XMLStreamException,
    AbsTTException {
    assert paramElem != null;

    // Initialize variables.
    initializeVars();

    // Main algorithm to determine if same, insert or a delete has to be made.
    algorithm(paramElem);

    if (mFound && mIsRightSibling) {
      mDelete = EDelete.ATSTARTMIDDLE;
      deleteNode();
    } else if (!mFound) {
      // Increment levels.
      mLevelInToShredder++;
      mLevelInShreddered++;

      insertElementNode(paramElem);
    } else if (mFound) {
      // Increment levels.
      mLevelInToShredder++;
      mLevelInShreddered++;

      sameElementNode();
    }
  }

  /**
   * Process characters.
   * 
   * @param paramText
   *          {@link Characters} currently parsed.
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   * @throws IOException
   *           In case of any I/O error.
   * @throws AbsTTException
   *           In case of any sirix error.
   */
  private void processCharacters(final Characters paramText) throws IOException, XMLStreamException,
    AbsTTException {
    assert paramText != null;
    // Initialize variables.
    initializeVars();
    final String text = paramText.getData().toString();
    if (!text.isEmpty()) {
      // Main algorithm to determine if same, insert or a delete has to be made.
      algorithm(paramText);

      if (mFound && mIsRightSibling) {
        /*
         * Cannot happen because if text node after end tag get's deleted it's done already while
         * parsing the end tag. If text node should be deleted at the top of a subtree (right after
         * a start tag has been parsed) it's done in processStartTag(StartElement).
         */
        // mDelete = EDelete.ATSTARTMIDDLE;
        // deleteNode();
        throw new AssertionError("");
      } else if (!mFound) {
        insertTextNode(paramText);
      } else if (mFound) {
        sameTextNode();
      }
    }
  }

  /**
   * Process end tag.
   * 
   * @throws XMLStreamException
   *           In case of any parsing error.
   * @throws AbsTTException
   *           In case anything went wrong while moving/deleting nodes in sirix.
   */
  private void processEndTag() throws XMLStreamException, AbsTTException {
    mLevelInToShredder--;

    if (mInserted) {
      mInsertedEndTag = true;
    }

    if (mRemovedNode) {
      mRemovedNode = false;
    } else {
      mLevelInShreddered--;

      // Move cursor to parent.
      if (mWtx.getNode().getNodeKey() == mLastNodeKey) {
        /*
         * An end tag must have been parsed immediately before and it must have been an empty element
         * at the end of a subtree, thus move this time to parent node.
         */
        assert mWtx.getNode().hasParent() && mWtx.getNode().getKind() == EKind.ELEMENT;
        mWtx.moveToParent();
      } else {
        if (mWtx.getNode().getKind() == EKind.ELEMENT) {
          final ElementNode element = (ElementNode)mWtx.getNode();
          if (element.hasFirstChild() && element.hasParent()) {
            // It's not an empty element, thus move to parent.
            mWtx.moveToParent();
          }
          // } else {
          // checkIfLastNode(true);
          // }
        } else if (mWtx.getStructuralNode().hasParent()) {
          if (mWtx.getStructuralNode().hasRightSibling()) {
            mWtx.moveToRightSibling();
            /*
             * Means next event is an end tag in StAX reader, but something different where the
             * sirix transaction points to, which also means it has to be deleted.
             */
            mKeyMatches = -1;
            mDelete = EDelete.ATBOTTOM;
            deleteNode();
          }
          mWtx.moveToParent();
        }

      }

      mLastNodeKey = mWtx.getNode().getNodeKey();

      // Move cursor to right sibling if it has one.
      if (mWtx.getStructuralNode().hasRightSibling()) {
        mWtx.moveToRightSibling();
        mMovedToRightSibling = true;

        skipWhitespaces(mReader);
        if (mReader.peek().getEventType() == XMLStreamConstants.END_ELEMENT) {
          /*
           * Means next event is an end tag in StAX reader, but something different where the
           * sirix transaction points to, which also means it has to be deleted.
           */
          mKeyMatches = -1;
          mDelete = EDelete.ATBOTTOM;
          deleteNode();
        }
      } else {
        mMovedToRightSibling = false;
      }
    }
  }

  /**
   * Main algorithm to determine if nodes are equal, have to be inserted, or
   * have to be removed.
   * 
   * @param paramEvent
   *          The currently parsed StAX event.
   * @throws IOException
   *           In case the open operation fails (delegated from
   *           checkDescendants(...)).
   * @throws XMLStreamException
   *           In case any StAX parser problem occurs.
   */
  private void algorithm(final XMLEvent paramEvent) throws IOException, XMLStreamException {
    assert paramEvent != null;
    do {
      /*
       * Check if a node in the shreddered file on the same level
       * equals the current element node.
       */
      if (paramEvent.isStartElement()) {
        mFound = checkElement(paramEvent.asStartElement());
      } else if (paramEvent.isCharacters()) {
        mFound = checkText(paramEvent.asCharacters());
      }
      if (mWtx.getNode().getNodeKey() != mNodeKey) {
        mIsRightSibling = true;
      }

      mKeyMatches = mWtx.getNode().getNodeKey();
      //
      // if (mFound && mIsRightSibling) {
      // /*
      // * Root element of next subtree in shreddered file matches
      // * so check all descendants. If they match the node must be
      // * inserted.
      // */
      // switch (paramEvent.getEventType()) {
      // case XMLStreamConstants.START_ELEMENT:
      // mMoved = EMoved.FIRSTNODE;
      // //mFound = checkDescendants(paramEvent.asStartElement());
      // mFound = checkDescendants(paramEvent.asStartElement());
      // break;
      // case XMLStreamConstants.CHARACTERS:
      // mFound = checkText(paramEvent.asCharacters());
      // break;
      // default:
      // // throw new AssertionError("Node type not known or not implemented!");
      // }
      // mWtx.moveTo(mKeyMatches);
      // }
    } while (!mFound && mWtx.moveToRightSibling());
    mWtx.moveTo(mNodeKey);
  }

  /**
   * Check if text event and text in sirix storage match.
   * 
   * @param paramEvent
   *          {@link XMLEvent}.
   * @return true if they match, otherwise false.
   */
  private boolean checkText(final Characters paramEvent) {
    assert paramEvent != null;
    final String text = paramEvent.getData().trim();
    return mWtx.getNode().getKind() == EKind.TEXT && mWtx.getValueOfCurrentNode().equals(text);
  }

  /**
   * In case they are the same nodes move cursor to next node and update
   * stack.
   * 
   * @throws TTIOException
   *           In case of any sirix error.
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   */
  private void sameTextNode() throws TTIOException, XMLStreamException {
    // Update variables.
    mInsert = EInternalInsert.NOINSERT;
    mDelete = EDelete.NODELETE;
    mInserted = false;
    mInsertedEndTag = false;
    mRemovedNode = false;

    // Check if last node reached.
    // checkIfLastNode(false);

    // Skip whitespace events.
    skipWhitespaces(mReader);

    // Move to right sibling if next node isn't an end tag.
    if (mReader.peek().getEventType() != XMLStreamConstants.END_ELEMENT) {
      // // Check if next node matches or not.
      // boolean found = false;
      // if (mReader.peek().getEventType() == XMLStreamConstants.START_ELEMENT) {
      // found = checkElement(mReader.peek().asStartElement());
      // } else if (mReader.peek().getEventType() == XMLStreamConstants.CHARACTERS) {
      // found = checkText(mReader.peek().asCharacters());
      // }
      //
      // // If next node doesn't match/isn't the same move on.
      // if (!found) {
      if (mWtx.moveToRightSibling()) {
        mMovedToRightSibling = true;
      } else {
        mMovedToRightSibling = false;
      }
      // }
    }

    mInsert = EInternalInsert.ATMIDDLEBOTTOM;
  }

  // /**
  // * Check if it's the last node in the shreddered file and modify flag mIsLastNode
  // * if it is the last node.
  // *
  // * @param paramDeleted
  // * Determines if method is invoked inside deleteNode()
  // */
  // private void checkIfLastNode(final boolean paramDeleted) {
  // // Last node or not?
  // int level = mLevelInShreddered;
  //
  // if (paramDeleted && level == 1 && mWtx.getItem().getKind() == ENodes.ELEMENT_KIND
  // && mWtx.getQNameOfCurrentNode().equals(mRootElem) && level == 1) {
  // mIsLastNode = true;
  // }
  //
  // if (!mIsLastNode) {
  // if (paramDeleted && level == 1) {
  // level++;
  // }
  // if (level > 0) {
  // final long nodeKey = mWtx.getItem().getKey();
  // while (!mWtx.getStructuralNode().hasRightSibling() && level != 0) {
  // mWtx.moveToParent();
  // level--;
  // if (mWtx.getItem().getKind() == ENodes.ELEMENT_KIND
  // && mWtx.getQNameOfCurrentNode().equals(mRootElem) && level == 1) {
  // mIsLastNode = true;
  // break;
  // }
  // }
  // mWtx.moveTo(nodeKey);
  // }
  // }
  // }

  /**
   * Nodes match, thus update stack and move cursor to first child if it is not a leaf node.
   * 
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   * @throws AbsTTException
   *           In case anything went wrong while moving the sirix transaction.
   */
  private void sameElementNode() throws XMLStreamException, AbsTTException {
    // Update variables.
    mInsert = EInternalInsert.NOINSERT;
    mDelete = EDelete.NODELETE;
    mInserted = false;
    mInsertedEndTag = false;
    mRemovedNode = false;

    // Check if last node reached.
    // checkIfLastNode(false);

    // Skip whitespace events.
    skipWhitespaces(mReader);

    // Move transaction.
    final ElementNode element = (ElementNode)mWtx.getNode();

    if (element.hasFirstChild()) {
      /*
       * If next event needs to be inserted, it has to be inserted at the top of the subtree, as first
       * child.
       */
      mInsert = EInternalInsert.ATTOP;
      mWtx.moveToFirstChild();

      if (mReader.peek().getEventType() == XMLStreamConstants.END_ELEMENT) {
        /*
         * Next event is an end tag, so the current child element, where the transaction currently is
         * located needs to be removed.
         */
        mKeyMatches = -1;
        mDelete = EDelete.ATBOTTOM;
        deleteNode();
      }
      // } else if (mReader.peek().getEventType() == XMLStreamConstants.END_ELEMENT
      // && !mReader.peek().asEndElement().getName().equals(mWtx.getQNameOfCurrentNode())) {
      // /*
      // * Node must be removed when next end tag doesn't match the current name and it has no children.
      // */
      // mKeyMatches = -1;
      // mDelete = EDelete.ATBOTTOM;
      // deleteNode();
    } else if (mReader.peek().getEventType() != XMLStreamConstants.END_ELEMENT) {
      /*
       * sirix transaction can't find a child node, but StAX parser finds one, so it must be inserted
       * as a first child of the current node.
       */
      mInsert = EInternalInsert.ATTOP;
      mEmptyElement = true;
    } else {
      mInsert = EInternalInsert.ATMIDDLEBOTTOM;
    }
  }

  /**
   * Skip any whitespace event.
   * 
   * @param paramReader
   *          the StAX {@link XMLEventReader} to use
   * @throws XMLStreamException
   *           if any parsing error occurs while moving the StAX parser
   */
  private void skipWhitespaces(final XMLEventReader paramReader) throws XMLStreamException {
    while (paramReader.peek().getEventType() == XMLStreamConstants.CHARACTERS
      && paramReader.peek().asCharacters().isWhiteSpace()) {
      paramReader.nextEvent();
    }
  }

  /**
   * Insert an element node.
   * 
   * @param paramElement
   *          {@link StartElement}, which is going to be inserted.
   * @throws AbsTTException
   *           In case any exception occurs while moving the cursor or
   *           deleting nodes in sirix.
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   */
  private void insertElementNode(final StartElement paramElement) throws AbsTTException, XMLStreamException {
    assert paramElement != null;
    /*
     * Add node if it's either not found among right siblings (and the
     * cursor on the shreddered file is on a right sibling) or if it's not
     * found in the structure and it is a new last right sibling.
     */
    mDelete = EDelete.NODELETE;
    mRemovedNode = false;

    switch (mInsert) {
    case ATTOP:
      // We are at the top of a subtree, no end tag has been parsed before.
      if (!mEmptyElement) {
        // Has to be inserted on the parent node.
        mWtx.moveToParent();
      }

      // Insert element as first child.
      addNewElement(EAdd.ASFIRSTCHILD, paramElement);
      mInsert = EInternalInsert.INTERMEDIATE;
      break;
    case INTERMEDIATE:
      // Inserts have been made before.
      EAdd insertNode = EAdd.ASFIRSTCHILD;

      if (mInsertedEndTag) {
        /*
         * An end tag has been read while inserting, thus insert node as right sibling of parent node.
         */
        mInsertedEndTag = false;
        insertNode = EAdd.ASRIGHTSIBLING;
      }

      // Possibly move one sibling back if transaction already moved to next node.
      if (mMovedToRightSibling) {
        mWtx.moveToLeftSibling();
      }

      // Make sure if transaction is on a text node the node is inserted as a right sibling.
      if (mWtx.getNode().getKind() == EKind.TEXT) {
        insertNode = EAdd.ASRIGHTSIBLING;
      }

      addNewElement(insertNode, paramElement);
      break;
    case ATMIDDLEBOTTOM:
      // Insert occurs at the middle or end of a subtree.

      // Move one sibling back.
      if (mMovedToRightSibling) {
        mMovedToRightSibling = false;
        mWtx.moveToLeftSibling();
      }

      // Insert element as right sibling.
      addNewElement(EAdd.ASRIGHTSIBLING, paramElement);
      mInsert = EInternalInsert.INTERMEDIATE;
      break;
    default:
      throw new AssertionError("Enum value not known!");
    }

    mInserted = true;
  }

  /**
   * Insert a text node.
   * 
   * @param paramText
   *          {@link Characters}, which is going to be inserted.
   * @throws AbsTTException
   *           In case any exception occurs while moving the cursor or
   *           deleting nodes in sirix.
   * @throws XMLStreamException
   *           In case of any StAX parsing error.
   */
  private void insertTextNode(final Characters paramText) throws AbsTTException, XMLStreamException {
    assert paramText != null;
    /*
     * Add node if it's either not found among right siblings (and the
     * cursor on the shreddered file is on a right sibling) or if it's not
     * found in the structure and it is a new last right sibling.
     */
    mDelete = EDelete.NODELETE;
    mRemovedNode = false;

    switch (mInsert) {
    case ATTOP:
      // Insert occurs at the top of a subtree (no end tag has been parsed immediately before).

      // Move to parent.
      mWtx.moveToParent();

      // Insert as first child.
      addNewText(EAdd.ASFIRSTCHILD, paramText);

      // Move to next node if no end tag follows (thus cursor isn't moved to parent in processEndTag()).
      if (mReader.peek().getEventType() != XMLStreamConstants.END_ELEMENT) {
        if (mWtx.moveToRightSibling()) {
          mMovedToRightSibling = true;
        } else {
          mMovedToRightSibling = false;
        }
      } else if (mWtx.getStructuralNode().hasRightSibling()) {
        mMovedToRightSibling = false;
        mInserted = true;
        mKeyMatches = -1;
        mDelete = EDelete.ATBOTTOM;
        deleteNode();
      }
      mInsert = EInternalInsert.INTERMEDIATE;
      break;
    case INTERMEDIATE:
      // Inserts have been made before.

      EAdd addNode = EAdd.ASFIRSTCHILD;

      if (mInsertedEndTag) {
        /*
         * An end tag has been read while inserting, so move back to left sibling if there is one and
         * insert as right sibling.
         */
        if (mMovedToRightSibling) {
          mWtx.moveToLeftSibling();
        }
        addNode = EAdd.ASRIGHTSIBLING;
        mInsertedEndTag = false;
      }

      // Insert element as right sibling.
      addNewText(addNode, paramText);

      // Move to next node if no end tag follows (thus cursor isn't moved to parent in processEndTag()).
      if (mReader.peek().getEventType() != XMLStreamConstants.END_ELEMENT) {
        if (mWtx.moveToRightSibling()) {
          mMovedToRightSibling = true;
        } else {
          mMovedToRightSibling = false;
        }
      }
      break;
    case ATMIDDLEBOTTOM:
      // Insert occurs in the middle or end of a subtree.

      // Move one sibling back.
      if (mMovedToRightSibling) {
        mWtx.moveToLeftSibling();
      }

      // Insert element as right sibling.
      addNewText(EAdd.ASRIGHTSIBLING, paramText);

      // Move to next node.
      mWtx.moveToRightSibling();

      mInsert = EInternalInsert.INTERMEDIATE;
      break;
    default:
      throw new AssertionError("Enum value not known!");
    }

    mInserted = true;
  }

  /**
   * Delete node.
   * 
   * @throws AbsTTException
   *           In case any exception occurs while moving the cursor or
   *           deleting nodes in sirix.
   */
  private void deleteNode() throws AbsTTException {
    /*
     * If found in one of the rightsiblings in the current shreddered
     * structure remove all nodes until the transaction points to the found
     * node (keyMatches).
     */
    if (mInserted && !mMovedToRightSibling) {
      mInserted = false;
      if (mWtx.getStructuralNode().hasRightSibling()) {
        // Cursor is on the inserted node, so move to right sibling.
        mWtx.moveToRightSibling();
      }
    }

    // // Check if transaction is on the last node in the shreddered file.
    // checkIfLastNode(true);

    // Determines if transaction has moved to the parent node after a delete operation.
    boolean movedToParent = false;

    // Determines if ldeleteNodeast node in a subtree is going to be deleted.
    boolean isLast = false;

    do {
      if (mWtx.getNode().getNodeKey() != mKeyMatches) {
        final IStructNode node = mWtx.getStructuralNode();
        if (!node.hasRightSibling() && !node.hasLeftSibling()) {
          if (mDelete == EDelete.ATSTARTMIDDLE) {
            // If the delete occurs right before an end tag the level hasn't been incremented.
            mLevelInShreddered--;
          }
          /*
           * Node has no right and no left sibling, so the transaction moves to the parent after the
           * delete.
           */
          movedToParent = true;
        } else if (!node.hasRightSibling()) {
          // Last node has been reached, which means that the transaction moves to the left sibling.
          isLast = true;
        }

        mWtx.remove();
      }
    } while (mWtx.getNode().getNodeKey() != mKeyMatches && !movedToParent && !isLast);

    if (movedToParent) {
      if (mDelete == EDelete.ATBOTTOM) {
        /*
         * Deleted right before an end tag has been parsed, thus don't move transaction to next
         * node in processEndTag().
         */
        mRemovedNode = true;
      }
      /*
       * sirix transaction has been moved to parent, because all child nodes have been deleted, thus
       * to right sibling.
       */
      mWtx.moveToRightSibling();
    } else {
      if (mWtx.getStructuralNode().hasFirstChild()) {
        if (mDelete == EDelete.ATBOTTOM && isLast) {
          /*
           * Deleted right before an end tag has been parsed, thus don't move transaction to next
           * node in processEndTag().
           */
          mRemovedNode = true;
        }

        if (isLast) {
          // If last node of a subtree has been removed, move to parent and right sibling.
          mWtx.moveToParent();
          mWtx.moveToRightSibling();

          // If the delete occurs right before an end tag the level hasn't been incremented.
          if (mDelete == EDelete.ATSTARTMIDDLE) {
            mLevelInShreddered--;
          }
        }
      }
    }

    // Check if transaction is on the last node in the shreddered file.
    // checkIfLastNode(true);
    mInsert = EInternalInsert.NOINSERT;
  }

  /**
   * Initialize variables needed for the main algorithm.
   */
  private void initializeVars() {
    mNodeKey = mWtx.getNode().getNodeKey();
    mFound = false;
    mIsRightSibling = false;
    mKeyMatches = -1;
  }

  /**
   * Add a new text node.
   * 
   * @param paramAdd
   *          determines how to add the node
   * @param paramTextEvent
   *          the current {@link Character} event from the StAX parser.
   * @throws AbsTTException
   *           if adding text node fails
   */
  private void addNewText(final EAdd paramAdd, final Characters paramTextEvent) throws AbsTTException {
    assert paramTextEvent != null;
    final String text = paramTextEvent.getData().trim();
    final ByteBuffer textByteBuffer = ByteBuffer.wrap(TypedValue.getBytes(text));
    if (textByteBuffer.array().length > 0) {
      if (paramAdd == EAdd.ASFIRSTCHILD) {
        mWtx.insertTextAsFirstChild(new String(textByteBuffer.array()));
      } else {
        mWtx.insertTextAsRightSibling(new String(textByteBuffer.array()));
      }
    }
  }

  /**
   * Add a new element node.
   * 
   * @param paramAdd
   *          determines wether node is added as first child or right sibling
   * @param paramStartElement
   *          the current {@link StartElement}
   * @throws AbsTTException
   *           if inserting node fails
   */
  private void addNewElement(final EAdd paramAdd, final StartElement paramStartElement) throws AbsTTException {
    assert paramStartElement != null;
    final QName name = paramStartElement.getName();
    long key;

    if (mFirstChildAppend == EInsert.ASRIGHTSIBLING) {
      key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
    } else {
      if (paramAdd == EAdd.ASFIRSTCHILD) {
        key = mWtx.insertElementAsFirstChild(name).getNode().getNodeKey();
      } else {
        key = mWtx.insertElementAsRightSibling(name).getNode().getNodeKey();
      }
    }

    // Parse namespaces.
    for (final Iterator<?> it = paramStartElement.getNamespaces(); it.hasNext();) {
      final Namespace namespace = (Namespace)it.next();
      mWtx.insertNamespace(new QName(namespace.getNamespaceURI(), "", namespace.getPrefix()));
      mWtx.moveTo(key);
    }

    // Parse attributes.
    for (final Iterator<?> it = paramStartElement.getAttributes(); it.hasNext();) {
      final Attribute attribute = (Attribute)it.next();
      mWtx.insertAttribute(attribute.getName(), attribute.getValue());
      mWtx.moveTo(key);
    }
  }

  /**
   * Check if descendants match. Beware of the fact that you have to move the write transaction to the
   * node it was before this method has been invoked.
   * 
   * @param paramElem
   *          the start element where the StAX parser currently is
   * @return true if they match, otherwise false
   * @throws XMLStreamException
   *           if streamining exception in the source document
   * @throws IOException
   *           if any I/O exception while opening the target file
   */
  private boolean checkDescendants(final StartElement paramElem) throws XMLStreamException, IOException {
    assert paramElem != null;
    boolean found = false;
    boolean lastToCheck = false;
    if (mMoved == EMoved.FIRSTNODE) {
      // Initialize level.
      mDescendantLevel = 0;
      /*
       * Setup new StAX parser and move it to the node, where the current
       * StAX parser is.
       */
      if (mFile != null) {
        mParser = createFileReader(mFile);
      } else if (mEvents != null) {
        mParser = createQueueReader(mEvents);
      } else {
        throw new IllegalStateException("Your XMLEventReader implementation is not supported!");
      }

      long elemsParsed = 0;
      XMLEvent event = null;
      while (mParser.hasNext() && elemsParsed != mElemsParsed) {
        event = mParser.nextEvent();
        if (event.isCharacters() && event.asCharacters().isWhiteSpace()) {
          continue;
        }
        elemsParsed++;
      }

      assert event != null && event.isStartElement()
        && event.asStartElement().getName().equals(paramElem.getName());

      if (event.isStartElement()) {
        found = checkElement(event.asStartElement());
      } else if (event.isCharacters()) {
        found = checkText(event.asCharacters());
      }
    }

    assert mParser != null;

    // Move write transaction to next node.
    boolean moved = false;
    final IStructNode node = mWtx.getStructuralNode();

    if (mMoved == EMoved.TOPARENT) {
      mMoved = EMoved.NOTTOPARENT;
      moved = true;
    } else {
      if (node.hasFirstChild()) {
        moved = mWtx.moveToFirstChild();
        mDescendantLevel++;
      } else if (node.hasRightSibling()) {
        moved = mWtx.moveToRightSibling();
      } else {
        moved = true;
      }
    }

    // Check if nodes are equal.
    if (moved) {
      mMoved = EMoved.NOTTOPARENT;
      if (mParser.hasNext()) {
        skipWhitespaces(mParser);
        final XMLEvent event = mParser.nextEvent();

        switch (event.getEventType()) {
        case XMLStreamConstants.START_ELEMENT:
          found = checkElement(event.asStartElement());
          break;
        case XMLStreamConstants.CHARACTERS:
          found = checkText(event.asCharacters());
          break;
        case XMLStreamConstants.END_ELEMENT:
          mMoved = EMoved.TOPARENT;
          if (mWtx.getNode().getKind() == EKind.ELEMENT) {
            // Move cursor to parent.
            if (mWtx.getNode().getNodeKey() == mLastDescCheckNodeKey) {
              /*
               * An end tag must have been parsed immediately before and it must have been an
               * empty element
               * at the end of a subtree, thus move this time to parent node.
               */
              assert mWtx.getNode().hasParent() && mWtx.getNode().getKind() == EKind.ELEMENT;
              found = mWtx.moveToParent();
              mDescendantLevel--;
            } else {
              if (mWtx.getStructuralNode().hasFirstChild()) {
                found = mWtx.moveToParent();
                mDescendantLevel--;
              } else {
                found = true;
                mLastDescCheckNodeKey = mWtx.getNode().getNodeKey();
              }
            }
          } else if (node.getKind() == EKind.TEXT) {
            found = mWtx.moveToParent();
            mDescendantLevel--;
          }

          if (mWtx.getNode().getKind() == EKind.ELEMENT
            && mWtx.getQNameOfCurrentNode().equals(paramElem.getName()) && mDescendantLevel == 0) {
            found = true;
            lastToCheck = true;
          }

          if (mWtx.getStructuralNode().hasRightSibling()) {
            found = mWtx.moveToRightSibling();
          }

          break;
        default:
          // Do nothing.
        }

        if (found && !lastToCheck) {
          checkDescendants(paramElem);
        }
      }
    }

    mParser.close();
    return found;
  }

  /**
   * Check if current element matches the element in the shreddered file.
   * 
   * @param mEvent
   *          StartElement event, from the XML file to shredder.
   * @return true if they are equal, false otherwise.
   */
  private boolean checkElement(final StartElement mEvent) {
    assert mEvent != null;
    boolean retVal = false;

    // Matching element names?
    if (mWtx.getNode().getKind() == EKind.ELEMENT
      && mWtx.getQNameOfCurrentNode().equals(mEvent.getName())) {
      // Check if atts and namespaces are the same.
      final long nodeKey = mWtx.getNode().getNodeKey();

      // Check attributes.
      boolean foundAtts = false;
      boolean hasAtts = false;
      for (final Iterator<?> it = mEvent.getAttributes(); it.hasNext();) {
        hasAtts = true;
        final Attribute attribute = (Attribute)it.next();
        for (int i = 0, attCount = ((ElementNode)mWtx.getNode()).getAttributeCount(); i < attCount; i++) {
          mWtx.moveToAttribute(i);
          if (attribute.getName().equals(mWtx.getQNameOfCurrentNode())
            && attribute.getValue().equals(mWtx.getValueOfCurrentNode())) {
            foundAtts = true;
            mWtx.moveTo(nodeKey);
            break;
          }
          mWtx.moveTo(nodeKey);
        }

        if (!foundAtts) {
          break;
        }
      }
      if (!hasAtts && ((ElementNode)mWtx.getNode()).getAttributeCount() == 0) {
        foundAtts = true;
      }

      // Check namespaces.
      boolean foundNamesps = false;
      boolean hasNamesps = false;
      for (final Iterator<?> namespIt = mEvent.getNamespaces(); namespIt.hasNext();) {
        hasNamesps = true;
        final Namespace namespace = (Namespace)namespIt.next();
        for (int i = 0, namespCount = ((ElementNode)mWtx.getNode()).getNamespaceCount(); i < namespCount; i++) {
          mWtx.moveToNamespace(i);
          final INameNode namenode = (INameNode)mWtx.getNode();
          if (namespace.getNamespaceURI().equals(mWtx.nameForKey(namenode.getURIKey()))
            && namespace.getPrefix().equals(mWtx.nameForKey(namenode.getNameKey()))) {
            foundNamesps = true;
            mWtx.moveTo(nodeKey);
            break;
          }
          mWtx.moveTo(nodeKey);
        }

        if (!foundNamesps) {
          break;
        }
      }
      if (!hasNamesps && ((ElementNode)mWtx.getNode()).getNamespaceCount() == 0) {
        foundNamesps = true;
      }

      // Check if atts and namespaces are the same.
      if (foundAtts && foundNamesps) {
        retVal = true;
      } else {
        retVal = false;
      }
    }

    return retVal;
  }

  /**
   * Main method.
   * 
   * @param args
   *          input and output files
   */
  public static void main(final String[] args) {
    if (args.length != 2) {
      throw new IllegalArgumentException("Usage: XMLShredder input.xml output.tnk");
    }

    LOGWRAPPER.info("Shredding '" + args[0] + "' to '" + args[1] + "' ... ");
    final long time = System.currentTimeMillis();
    final File target = new File(args[1]);

    try {
      final DatabaseConfiguration config = new DatabaseConfiguration(target);
      Database.createDatabase(config);
      final IDatabase db = Database.openDatabase(target);
      db.createResource(new ResourceConfiguration.Builder("shredded", config).build());
      final ISession session = db.getSession(new SessionConfiguration.Builder("shredded").build());
      final INodeWriteTrx wtx = session.beginNodeWriteTrx();
      final XMLEventReader reader = createFileReader(new File(args[0]));
      final XMLUpdateShredder shredder =
        new XMLUpdateShredder(wtx, reader, EInsert.ASFIRSTCHILD, new File(args[0]),
          EShredderCommit.COMMIT);
      shredder.call();

      wtx.close();
      session.close();
    } catch (final AbsTTException | XMLStreamException | IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    LOGWRAPPER.info(" done [" + (System.currentTimeMillis() - time) + "ms].");
  }

}
