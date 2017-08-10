// package org.sirix.index.bplustree;
//
// import static com.google.common.base.Preconditions.checkNotNull;
//
// import javax.annotation.Nonnull;
//
// import org.sirix.api.PageWriteTrx;
// import org.sirix.exception.SirixIOException;
// import org.sirix.page.PageKind;
//
// public final class BPlusTreeWriter<K extends Comparable<? super K>, V> implements
// AutoCloseable {
//
// private final BPlusTreePageWriteTrx mPageWriteTrx;
// private final PageKind mKind;
//
// private BPlusTreeWriter(final BPlusTreePageWriteTrx pageWriteTrx,
// final PageKind kind) {
// mPageWriteTrx = pageWriteTrx;
// mKind = kind;
// }
//
// /**
// * Get a new instance.
// *
// * @param pageWriteTrx
// * {@link PageWriteTrx} for persistent storage
// * @param kind
// * kind of page
// * @return new tree instance
// */
// public static <KE extends Comparable<? super KE>, VA> BPlusTreeWriter<KE, VA> getInstance(
// final BPlusTreePageWriteTrx pageWriteTrx, final PageKind kind) {
// return new BPlusTreeWriter<KE, VA>(checkNotNull(pageWriteTrx), checkNotNull(kind));
// }
//
// /**
// * Closes the tree storage.
// *
// * @throws SirixIOException
// * I/O exception if closing the BPlusTree fails
// */
// @Override
// public void close() throws SirixIOException {
//
// }
//
// //
// // /* ======================== Class Methods ======================== */
// //
// // /**
// // * Returns true if the specified value was found.
// // * @param value value to be found
// // * @return true if the value was found
// // * @throws IOException I/O exception
// // */
// // public boolean contains(final V value) throws IOException {
// // return contains(value, 0);
// // }
// //
// // /**
// // * Inserts a value into the tree structure.
// // * If -1 is returned, everything went alright.
// // * If -2 is returned, the value is already stored.
// // * @param value value to be added
// // * @throws IOException I/O exception
// // */
// // public void insert(final V value) throws IOException {
// // if(insert(value, 0) == -1) storage.nrValues++;
// // }
// //
// // /**
// // * Deletes a value from the tree structure.
// // * If -1 is returned, everything went alright.
// // * If -2 is returned, the value was not found.
// // * @param value value to be deleted
// // * @throws IOException I/O exception
// // */
// // public void delete(final V value) throws IOException {
// // if(delete(value, storage.root) == -1) storage.nrValues--;
// // }
// //
// // /**
// // * Returns the number of currently stored values.
// // * @return number of stored values
// // */
// // public int nrValues() {
// // return storage.nrValues;
// // }
// //
// //
// // /**
// // * Dumps the contents of the disk storage to the specified output stream.
// // * @param out output stream
// // * @throws IOException I/O exception
// // */
// // public void dump(final PrintStream out) throws IOException {
// // storage.dump(out);
// // }
// //
// // /**
// // * Recursive method for finding the specified value. The boolean
// // * value <code>true</code> is returned if the specified value was found
// // * in a leaf node.
// // * @param value value to be found
// // * @param pointer pointer to the current block
// // * @return true if value has been found; false otherwise
// // * @throws IOException I/O exception
// // */
// // private boolean contains(final V value, final long pointer) throws
// // IOException {
// // // get block from disk
// // mPageWriteTrx.
// // final BTreeBlock block = storage.read(pointer);
// // final int size = block.nrValues;
// //
// // // check if the current block is a leaf node
// // if(block.leaf) {
// // // check if the leaf value is found
// // int pos = -1;
// // while(++pos < size) {
// // // value was found - unpin block and return true
// // if(value == block.getLeaf(pos)) {
// // block.unpin();
// // return true;
// // }
// // }
// // // value was not found - unpin block and return false
// // block.unpin();
// // return false;
// // }
// //
// // // no leaf - find child block
// // int pos = -1;
// // while(++pos < size) {
// // // found value is smaller or equal - stop searching
// // if(value < block.getValue(pos)) break;
// // }
// //
// // // continue with child block
// // final int child = block.getChild(pos);
// // block.unpin();
// // return contains(value, child);
// // }
// //
// }
