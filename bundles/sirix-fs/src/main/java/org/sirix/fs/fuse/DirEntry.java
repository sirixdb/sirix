package org.sirix.fs.fuse;

/**
 * Directory entry.
 */
final class DirEntry {
  /** Database nodeKey/FS Inode. */
  final long mNodeKey;
  /** Name for id. */
  final byte[] mDirname;

  /**
   * Constructor.
   * 
   * @param pNodeKey
   *          nodeKey/FS Inode
   * @param pDirname
   *          name of node/directory name
   */
  DirEntry(final long pNodeKey, final byte[] pDirname) {
    mNodeKey = pNodeKey;
    mDirname = pDirname;
  }
}
