package org.sirix.encryption;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.Stack;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.sirix.exception.SirixEncryptionException;

/**
 * Class for parsing the initial given right tree and storing
 * all data into the databases.
 * 
 * @author Patrick Lang, University of Konstanz
 */
public class EncryptionTreeParser extends DefaultHandler {

  /**
   * Path of initial right tree XML file.
   */
  private static final String FILENAME = "src" + File.separator + "test" + File.separator + "resources"
    + File.separator + "righttreestructure.xml";

  /**
   * Instance for {@link KeySelectorDatabase}.
   */
  private static KeySelectorDatabase mSelectorDb;

  /**
   * Instance for {@link KeyMaterialDatabase}.
   */
  private static KeyMaterialDatabase mMaterialDb;

  /**
   * Instance for {@link KeyManagerDatabase}.
   */
  private static KeyManagerDatabase mManagerDb;

  /**
   * Node declaration in initial right tree XML file.
   */
  private final String mNodeDec = "NODE";

  /**
   * Edge declaration in initial right tree XML file.
   */
  private final String mEdgeDec = "EDGE";

  /**
   * User type declaration in initial right tree XML file.
   */
  private final String mTypeUser = "user";

  /**
   * Stack holding all parsed nodes.
   */
  private final Stack<Long> mNodeStack = new Stack<Long>();

  /**
   * Map holding all node names and its corresponding node ids.
   */
  private final Map<String, Long> mNodeIds = new HashMap<String, Long>();

  /**
   * List of all parsed users.
   */
  private final List<String> mUsers = new ArrayList<String>();

  /**
   * Just a helper map for user that has parent that hasn't been parsed
   * and written to the database yet.
   */
  private final Map<Long, List<String>> mUserParents = new HashMap<Long, List<String>>();

  /**
   * Start tree parsing process.
   * 
   * @param selDb
   *          key selector database instance.
   * @param matDb
   *          keying material database instance.
   * @param manDb
   *          key manager database instance.
   */
  public final void init(final KeySelectorDatabase selDb, final KeyMaterialDatabase matDb,
    final KeyManagerDatabase manDb) {

    mSelectorDb = selDb;
    mMaterialDb = matDb;
    mManagerDb = manDb;

    try {

      SAXParserFactory factory = SAXParserFactory.newInstance();
      SAXParser saxParser = factory.newSAXParser();

      saxParser.parse(FILENAME, new EncryptionTreeParser());

    } catch (final Exception mExp) {
      mExp.printStackTrace();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void startElement(final String namespaceURI, final String localName, final String qName,
    final Attributes atts) throws SAXException {

    final String mNodeName = atts.getValue(0);
    final String mNodeType = atts.getValue(1);

    if (qName.equals(mNodeDec)) {

      if (mNodeStack.size() > 0) {
        mNodeStack.pop();
      }

      final KeySelector mSelector;
      if (mNodeType.equals("group")) {
        mSelector = new KeySelector(mNodeName, EntityType.GROUP);
      } else {
        mSelector = new KeySelector(mNodeName, EntityType.USER);
      }

      mNodeStack.add(mSelector.getKeyId());
      mNodeIds.put(mNodeName, mSelector.getKeyId());

      mSelectorDb.putPersistent(mSelector);

      final KeyingMaterial mMaterial =
        new KeyingMaterial(mSelector.getKeyId(), mSelector.getRevision(), mSelector.getVersion(),
          new NodeEncryption().generateSecretKey());
      mMaterialDb.putPersistent(mMaterial);

      if (atts.getValue(1).equals(mTypeUser)) {
        mUsers.add(mNodeName);
      }

    } else if (qName.equals(mEdgeDec)) {
      long mNodeId = mNodeStack.peek();
      KeySelector mSelector = mSelectorDb.getPersistent(mNodeId);

      if (mNodeIds.containsKey(mNodeName)) {
        mSelector.addParent(mNodeIds.get(mNodeName));
        mSelectorDb.putPersistent(mSelector);
      } else {
        if (mUserParents.containsKey(mNodeId)) {
          List<String> mNameList = mUserParents.get(mNodeId);
          mNameList.add(mNodeName);
        } else {
          List<String> mNameList = new LinkedList<String>();
          mNameList.add(mNodeName);
          mUserParents.put(mNodeId, mNameList);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void endElement(final String namespaceURI, final String localName, final String qName)
    throws SAXException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void endDocument() throws SAXException {
    /*
     * add not yet stored edges as parents to the nodes respectively
     */
    Iterator iter = mUserParents.keySet().iterator();
    while (iter.hasNext()) {
      final long mMapKey = (Long)iter.next();
      final KeySelector mSelector = mSelectorDb.getPersistent(mMapKey);
      final List<String> mNameList = mUserParents.get(mMapKey);

      for (int i = 0; i < mNameList.size(); i++) {
        final String mName = mNameList.get(i);

        final SortedMap<Long, KeySelector> mSelMap = mSelectorDb.getEntries();
        final Iterator innerIter = mSelMap.keySet().iterator();

        while (innerIter.hasNext()) {

          final KeySelector mParentSelector = mSelMap.get(innerIter.next());

          if (mParentSelector.getName().equals(mName)) {
            mSelector.addParent(mParentSelector.getKeyId());
            mSelectorDb.putPersistent(mSelector);
          }
        }
      }
    }

    /*
     * build key trails of users and store it to the key manager db
     */

    // iterate through all users
    for (int j = 0; j < mUsers.size(); j++) {
      final String mUser = mUsers.get(j);

      // map of all key trails a user has
      final Map<Long, List<Long>> mKeyTrails = new HashMap<Long, List<Long>>();

      // find user node in selector db
      final SortedMap<Long, KeySelector> mSelMap = mSelectorDb.getEntries();
      final Iterator innerIter = mSelMap.keySet().iterator();

      while (innerIter.hasNext()) {
        final KeySelector mSelector = mSelMap.get(innerIter.next());
        if (mSelector.getName().equals(mUser)) {

          // all parent ids of user
          final List<Long> mParentIds = mSelector.getParents();

          for (int k = 0; k < mParentIds.size(); k++) {
            final long mParent = mParentIds.get(k);
            final LinkedList<Long> mKeyTrail = new LinkedList<Long>();
            mKeyTrail.add(mParent);
            mKeyTrails.put(mParent, mKeyTrail);
          }

          // iterate through all initiated key trails and find
          // and complete the trail steps to the root
          iter = mKeyTrails.keySet().iterator();
          while (iter.hasNext()) {
            final long mParentKey = (Long)iter.next();
            final List<Long> mKeyTrail = mKeyTrails.get(mParentKey);

            // get parent id from parent
            List<Long> mParentList = mSelectorDb.getPersistent(mParentKey).getParents();

            while (mParentList.size() != 0) {
              if (mParentList.size() > 1) {
                try {
                  throw new SirixEncryptionException("Initial right tree is not valid. A group node can "
                    + "only have one parent.");
                } catch (final SirixEncryptionException ttee) {
                  ttee.printStackTrace();
                }
              } else {
                // add parent's parent id to key trail
                final long newParent = mParentList.get(0);
                mKeyTrail.add(newParent);
                mParentList = mSelectorDb.getPersistent(newParent).getParents();
              }
            }
          }
        }
      }

      // add key trails to key manager database
      final KeyManager entity = new KeyManager(mUser, mKeyTrails);
      mManagerDb.putPersistent(entity);

    }
  }
}
