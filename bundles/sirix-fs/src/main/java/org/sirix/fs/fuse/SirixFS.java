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
package org.sirix.fs.fuse;

/// **
// * Platform independent FUSE part (native part is provided by sirixfs library).
// *
// * @author Johannes Lichtenberger, University of Konstanz
// *
// */
public class SirixFS {
  //
  // static {
  // System.loadLibrary("sirixfs");
  // }
  //
  // /**
  // * Expected number of returned st_* atts from FSML entries.
  // * (This static value is also read by the native library on load.)
  // *
  // * [0] st_mode (octal)
  // * [1] st_size
  // * [2] st_mtime
  // * [3] st_atime
  // * [4] st_ctime
  // * [5] st_uid
  // * [6] st_gid
  // * [7] st_nlink
  // * [8] st_ino
  // */
  // private static final int NUMBER_OF_STAT_ATTS = 9;
  //
  // /** Singleton instance. */
  // private static SirixFS mInstance;
  //
  // /** sirix {@link INodeWriteTrx}. */
  // private final INodeWriteTrx mWtx;
  //
  // /** {@link LogWrapper} reference. */
  // private static final LogWrapper LOGWRAPPER = new
  // LogWrapper(LoggerFactory.getLogger(SirixFS.class));
  //
  // /** Single thread thread pool. */
  // private final ExecutorService mPool = Executors.newSingleThreadExecutor();
  //
  // private final File mPath;
  //
  // /**
  // * Private constructor which opens a {@link IWriteTeansaction} on the given
  // path. Note that only one
  // *
  // * @param pPath
  // * {@code path} to database
  // * @throws SirixException
  // * if anything fails while opening a new transaction
  // */
  // private SirixFS(final File pPath) throws SirixException {
  // mPath = checkNotNull(pPath);
  // final IDatabase database = Database.openDatabase(pPath);
  // mWtx = database.getSession(new
  // SessionConfiguration.Builder("shredded").build()).beginNodeWriteTrx();
  // }
  //
  // public static SirixFS getInstance(final File pPath) throws SirixException {
  // if (mInstance == null) {
  // mInstance = new SirixFS(pPath);
  // }
  // return mInstance;
  // }
  //
  // /* --------------------------------------------------------------------- */
  // /* --------- Calls to native code (j2c) -------------------------------- */
  // /* --------------------------------------------------------------------- */
  // private native void j2cInfo();
  //
  // private native boolean j2cMount(final int argc, final byte[][] argv);
  //
  // /** Prints some info about native library. */
  // public void info() {
  // j2cInfo();
  // }
  //
  // /** Reference thread running FUSE. */
  // private Thread mountThread;
  //
  // /** Thread to launch native FUSE mount operation with arguments. */
  // private class MountThread implements Runnable {
  // /** Arguments to mount the filesystem. */
  // private final String[] mArgs;
  //
  // /**
  // * Constructor.
  // *
  // * @param pArgs
  // * arguments to mount the filesystem
  // */
  // public MountThread(final String[] pArgs) {
  // mArgs = checkNotNull(pArgs);
  // }
  //
  // /** {@inheritDoc} */
  // @Override
  // public void run() {
  // final int argc = mArgs.length;
  // final byte[][] argv = new byte[argc][];
  //
  // for (int i = 0; i < argc; i++) {
  // argv[i] = mArgs[i].getBytes();
  // }
  //
  // j2cMount(argc, argv);
  // }
  // }
  //
  // /**
  // * Mounts FUSE with user arguments and loads FSML database into Server.
  // *
  // * Fuse4x options:
  // * - "fsname=sirixfs,volname=sirixFS,noappledouble,noapplexattr"
  // */
  // public boolean mount(final String database, final String mountpoint) {
  // final String[] args = {
  // "sirixfs", "-o", "fsname=sirixfs", mountpoint
  // };
  //
  // mPool.submit(new MountThread(args));
  // return false;
  // }
  //
  // /* --------------------------------------------------------------------- */
  // /* --------- Callbacks from native code (c2j) -------------------------- */
  // /* --------------------------------------------------------------------- */
  //
  // /**
  // * Noop callback from FUSE to test JNI overhead.
  // */
  // private void c2jNoop() {
  // /* Do nothing. */
  // LOGWRAPPER.debug("c2jNoop (java part)");
  // }
  //
  // /**
  // * Get file status.
  // *
  // * An absolute file pathname is passed in as input from native FUSE code. It
  // * always has the form {@code '/path/to/dir_or_file'} and belongs to an FSML
  // {code <file>}
  // * or {@code <dir>} element with {@code @st*} attributes (st_mode="0100644"
  // st_size="92652"
  // * st_mtime="0" st_atime="0" st_ctime="0" st_uid="1000" st_gid="1000"
  // * st_nlink="1"). The filesystem pathname is converted to an equivalent
  // * XPath expression working on a FSML database.
  // *
  // * @param path
  // * @return fixed size ({@link NUMBER_OF_STAT_ATTS}) array of longs to fill
  // * stat(2) struct in FUSE or null to indicate "No such file or
  // * directory" (ENOENT).
  // *
  // */
  // public long[] c2jGetattr(final long pIno) {
  // if (!mWtx.moveTo(pIno).hasMoved()) {
  // // No node key for Inode found.
  // return null;
  // }
  //
  // if (mWtx.getKind() == EKind.ELEMENT) {
  // return getLongFromElement();
  // } else {
  // return null;
  // }
  // }
  //
  // /**
  // * <p>
  // * Get a long-array with all stat attribute values.
  // * </p>
  // *
  // * <p>
  // * <strong>Precondition:</strong> Transaction must be located on an element
  // node.
  // * </p>
  // *
  // * @return long-array filled with stat attribute values.
  // */
  // private long[] getLongFromElement() {
  // assert mWtx.getKind() == EKind.ELEMENT :
  // "Transaction must be on an element!";
  //
  // // Safe to cast now.
  // final ElementNode element = (ElementNode)mWtx.getNode();
  //
  // // Store attributes in a map if any.
  // final Map<String, Long> attributes = getAttributes(element);
  //
  // // Store mapped attributes in the long array.
  // return getLongAtts(attributes);
  // }
  //
  // /**
  // * Get all attributes of an element.
  // *
  // * @param pElement
  // * {@link ElementNode} reference
  // * @return {@link Map} of {@code String} <=> {@code long} mapping
  // */
  // private Map<String, Long> getAttributes(final ElementNode pElement) {
  // final Map<String, Long> attributes = new HashMap<>();
  // final int nbAtts = pElement.getAttributeCount();
  // if (nbAtts > 0) {
  // for (int i = 0; i < nbAtts; i++) {
  // mWtx.moveToAttribute(i);
  // final String name =
  // mWtx.getQNameOfCurrentNode().getLocalPart().toLowerCase();
  // final long value =
  // "st_mode".equals(name) ? Long.parseLong(mWtx.getValueOfCurrentNode(), 8) :
  // Long.parseLong(mWtx
  // .getValueOfCurrentNode()); // Long.parseLong(String, 8): octal mode
  // // string
  // attributes.put(name, value);
  // mWtx.moveToParent();
  // if (i == nbAtts - 1) {
  // assert i == (NUMBER_OF_STAT_ATTS - 1) : "Unexpected number of attributes";
  // }
  // }
  // }
  // return attributes;
  // }
  //
  // /**
  // * Return a long mapping of the given {@link Map}.
  // *
  // * @param pAttributes
  // * {@link Map} with {@code String} <=> {@code Long} mapping
  // */
  // private long[] getLongAtts(final Map<String, Long> pAttributes) {
  // assert pAttributes != null;
  //
  // final long[] atts = new long[NUMBER_OF_STAT_ATTS];
  // atts[0] = pAttributes.get("st_mode");
  // atts[1] = pAttributes.get("st_size");
  // atts[2] = pAttributes.get("st_mtime");
  // atts[3] = pAttributes.get("st_atime");
  // atts[4] = pAttributes.get("st_ctime");
  // atts[5] = pAttributes.get("st_uid");
  // atts[6] = pAttributes.get("st_gid");
  // atts[7] = pAttributes.get("st_nlink");
  // atts[8] = pAttributes.get("st_ino");
  // return atts;
  // }
  //
  // /**
  // * Lookup a directory/file name.
  // *
  // * @param pParent
  // * the parent {@code Inode/nodeKey}
  // * @param pName
  // * the name to lookup
  // * @return st_* array with the values of each attribute
  // */
  // private long[] c2jLookup(final long pParent, final byte[] pName) {
  // if (pName == null) {
  // LOGWRAPPER.error("[c2jLookup] name: is null");
  // return null;
  // }
  //
  // LOGWRAPPER.debug("[c2jLookup] parent: " + pParent + " name: '" + new
  // String(pName) + "'");
  //
  // if (!mWtx.moveTo(pParent)) {
  // LOGWRAPPER.error("[c2jLookup] parent: not found");
  // return null;
  // }
  //
  // // Note: We need indexes.
  // final IFilter filter = new NameFilter(mWtx, new String(pName));
  // boolean found = false;
  // long nodeKey = -1;
  // for (final AbsAxis axis = new ChildAxis(mWtx); !found && axis.hasNext();
  // axis.next()) {
  // for (final AbsAxis attAxis = new AttributeAxis(mWtx); !found &&
  // attAxis.hasNext(); attAxis.next()) {
  // if (filter.filter()) {
  // mWtx.moveToParent();
  // assert mWtx.getNode().getKind() == EKind.ELEMENT :
  // "Transaction must be on an element!";
  // nodeKey = mWtx.getNode().getNodeKey();
  // found = true;
  // }
  // }
  // }
  //
  // if (found) {
  // assert nodeKey != -1 : "NodeKey must be valid!";
  // mWtx.moveTo(nodeKey);
  // assert mWtx.getNode().getKind() == EKind.ELEMENT :
  // "Transaction must be on an element!";
  // return getLongFromElement();
  // } else {
  // return null;
  // }
  // }
  //
  // private DirEntry[] c2jReaddir(final long pIno, final long pOff) {
  // LOGWRAPPER.debug("[c2jReaddir] ino: " + pIno + " off: " + pOff);
  //
  // if (!mWtx.moveTo(pIno)) {
  // LOGWRAPPER.error("[c2jReaddif] ino: not found");
  // return null;
  // }
  //
  // mWtx.moveToFirstChild();
  //
  // // Move to offset.
  // for (int i = 1; i < pOff; i++) {
  // assert mWtx.getStructuralNode().hasRightSibling() :
  // "Node must have a right sibling!";
  // mWtx.moveToRightSibling();
  // }
  //
  // // Note: We need indexes.
  // // Add nodes.
  // final List<DirEntry> files = new LinkedList<>();
  // while (mWtx.getStructuralNode().hasRightSibling()) {
  // boolean found = false;
  // for (final AbsAxis axis = new AttributeAxis(mWtx); !found &&
  // axis.hasNext(); axis.next()) {
  // if
  // ("name".equalsIgnoreCase(axis.getTransaction().getQNameOfCurrentNode().getLocalPart()))
  // {
  // found = true;
  // files.add(new DirEntry(mWtx.getNode().getParentKey(),
  // mWtx.getQNameOfCurrentNode().getLocalPart()
  // .getBytes()));
  // }
  // }
  //
  // mWtx.moveToRightSibling();
  // }
  //
  // return files.toArray(new DirEntry[files.size()]);
  // }
}
