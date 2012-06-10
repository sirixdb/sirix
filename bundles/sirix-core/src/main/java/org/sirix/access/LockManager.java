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

package org.sirix.access;

/**
 * <h1>LockManager</h1>
 * 
 * <h2>Description</h2>
 * 
 * <p>
 * Each <code>Session</code> owns a <code>LockManager</code> which keeps track of all
 * <code>WriteTransaction</code> and their respective transaction root nodes. The <code>LockManager</code>
 * checks for a new <code>WriteTransaction</code> if the requested subtree is currently free for use.
 * 
 */

public class LockManager {

  // // Locked nodes and transactionRootNodes of a LockManager
  // private final HashMap<Long, Integer> lockedNodes;
  // private final HashMap<SynchWriteTransaction, Set<Long>> transactionRootNodes;
  // private static LockManager lock;
  //
  // /**
  // * The LockManager with HashMaps of locked nodes and used transaction root
  // * nodes
  // */
  // private LockManager() {
  // this.lockedNodes = new HashMap<Long, Integer>(128);
  // this.transactionRootNodes = new HashMap<SynchWriteTransaction, Set<Long>>(128);
  // }
  //
  // public static LockManager getLockManager() {
  // if (lock == null) {
  // lock = new LockManager();
  // }
  //
  // return lock;
  // }
  //
  // /**
  // *
  // * @param nodekey
  // * ID of nodekey that is to be locked
  // * @param swtx
  // * The corresponing SynchWriteTransaction
  // */
  // public synchronized void getWritePermission(long nodekey, SynchWriteTransaction swtx) {
  // // lock parent node
  // if (swtx.moveToParent()) {
  // long current = nodekey;
  // nodekey = swtx.getCurrentNode().getKey();
  // swtx.moveTo(current);
  // }
  //
  // // first write operation or after intermediate commit, there may be no
  // // registered trn
  // if (!this.transactionRootNodes.containsKey(swtx)) {
  // // try to conquer the node - will throw exception if unsuccessfull
  // conquer(swtx, nodekey);
  // } else { // there is a registered trn for this session,
  // // but are we within our conquered part of the tree?
  // if (isInTransactionSubtree(swtx, nodekey)) {
  // return;
  // } else { // obviously we are not in the subtree
  // conquer(swtx, nodekey);
  // }
  // }
  // }
  //
  // /**
  // *
  // * @param swtx
  // * The locks of the Trn(s) of this SynchWriteTransaction will be
  // * released
  // */
  // public synchronized void releaseWritePermission(SynchWriteTransaction swtx) {
  // Set<Long> hset = transactionRootNodes.get(swtx);
  // if (hset == null || hset.isEmpty()) {
  // return;
  // }
  // Long[] trns = null;
  // trns = hset.toArray(new Long[0]);
  // for (Long trn : trns) {
  // unlockAncestors(swtx, trn);
  // }
  // transactionRootNodes.remove(swtx);
  // }
  //
  // private void conquer(SynchWriteTransaction swtx, long nodeToConquer) {
  //
  // if (!isConquerable(swtx, nodeToConquer)) {
  // throw new IllegalStateException();
  // }
  //
  // lockAncestors(swtx, nodeToConquer);
  // registerTransactionRootNodeKey(swtx, nodeToConquer);
  //
  // }
  //
  // /**
  // * isConquerable determines whether a node can be captured
  // *
  // * @param NodeToConquer
  // * @return common ancestor nodekey if available, throws exception if not
  // */
  // private boolean isConquerable(SynchWriteTransaction swtx, long NodeToConquer) {
  // /*
  // * (1)Node locked? (2) Yes:Do locks originate only from own swtx? (3)
  // * Yes:Unlock corresponding trns and lock nodeToConquer -> TRUE (4) No:
  // * Locking not possible: FALSE (5) No: Is there an ancestor of trn
  // * registered as trn? (6) Yes: Locking not possible: FALSE (7) No: Lock
  // * nodeToConquer TRUE
  // */
  //
  // if (lockedNodes.containsKey(NodeToConquer)) { // (1)
  // Set<Long> hset = transactionRootNodes.get(swtx); // own trns
  //
  // if (hset == null || hset.isEmpty()) {
  // return false;
  // }
  //
  // // Check if locks originate from own swtx (2)
  // int ownLockCount = 0;
  //
  // Long[] trns = null;
  // trns = hset.toArray(new Long[1]);
  //
  // for (Long trn : trns) {
  // swtx.moveTo(trn);
  // if (swtx.getCurrentNode().getKey() == NodeToConquer) {
  // ownLockCount++;
  // }
  // while (swtx.moveTo(swtx.getCurrentNode().getParentKey())) {
  // if (swtx.getCurrentNode().getKey() == NodeToConquer) {
  // ownLockCount++;
  // }
  // }
  // }
  // if (ownLockCount == lockedNodes.get(NodeToConquer)) { // (3)
  // return true;
  // } else { // (4)
  // return false;
  // }
  // }
  //
  // // (5)
  // swtx.moveTo(NodeToConquer);
  // Set<SynchWriteTransaction> keyset = transactionRootNodes.keySet(); // get
  // // all
  // // swtx
  // while (swtx.moveToParent()) {
  // Iterator iterate = keyset.iterator();
  // long currentNodekey = swtx.getCurrentNode().getKey();
  // while (iterate.hasNext()) {
  // Set valueset = transactionRootNodes.get(iterate.next());
  // if (valueset.contains(currentNodekey)) {
  // return false; // (6)
  // }
  // }
  // }
  //
  // return true; // (7)
  // }
  //
  // /*
  // * Increments the lock count of the nodekey <code>key</code> by 1. Creates
  // * new entry in HashMap if the key is not present yet.
  // *
  // * @param key The key of the node to lock
  // *
  // * @return
  // */
  // private void lockKey(long key) {
  // if (lockedNodes.containsKey(key)) {
  // lockedNodes.put(key, lockedNodes.get(key) + 1);
  // } else {
  // lockedNodes.put(key, 1);
  // }
  // }
  //
  // /*
  // * Decrements the lock count of the nodekey <code>key</code> by 1. Removes
  // * the entry in HashMap if the lock count would become 0.
  // *
  // * @param key The key of the node to unlock
  // */
  // private void unlockKey(long key) {
  // if (lockedNodes.containsKey(key)) {
  // if (lockedNodes.get(key) > 1) {
  // lockedNodes.put(key, lockedNodes.get(key) - 1);
  // return;
  // } else if (lockedNodes.get(key) == 1) {
  // lockedNodes.remove(key);
  // return;
  // }
  // }
  // throw new IllegalStateException("Key not in hashmap: " + key);
  // }
  //
  // /*
  // * Adds a nodekey to the list of transation root nodes
  // *
  // * @param key Nodekey to add to the list
  // */
  // private void registerTransactionRootNodeKey(SynchWriteTransaction swtx, long key) {
  // if (!this.transactionRootNodes.containsKey(swtx)) {
  // this.transactionRootNodes.put(swtx, new HashSet<Long>());
  // }
  // this.transactionRootNodes.get(swtx).add(key);
  // }
  //
  // /*
  // * checks if a node is within a subtree of the same wtx
  // */
  // private boolean isInTransactionSubtree(SynchWriteTransaction swtx, long nodeKey) {
  // long current = swtx.getCurrentNode().getKey();
  // if (transactionRootNodes.get(swtx).contains(nodeKey)) { // node is a trn
  // return true;
  // }
  // while (swtx.moveTo(swtx.getCurrentNode().getParentKey())) { // check
  // // ancestors
  // if (transactionRootNodes.get(swtx).contains(swtx.getCurrentNode().getKey())) {
  // swtx.moveTo(current);
  // return true;
  // }
  // }
  // swtx.moveTo(current); // not in subtree
  // return false;
  // }
  //
  // /*
  // * Locks nodeToLock and all its ancestors for the given session
  // */
  // private void lockAncestors(SynchWriteTransaction swtx, long nodeToLock) {
  // long current = swtx.getCurrentNode().getKey();
  // swtx.moveTo(nodeToLock);
  // // lock trk and all its parents
  // lockKey(nodeToLock);
  // if (swtx.moveTo(swtx.getCurrentNode().getParentKey())) {
  // lockKey(swtx.getCurrentNode().getKey());
  // }
  // swtx.moveTo(current);
  // }
  //
  // /*
  // * Unlocks nodeToUnlock and all its ancestors
  // */
  // private void unlockAncestors(SynchWriteTransaction swtx, long nodeToUnlock) {
  // long current = swtx.getCurrentNode().getKey();
  // swtx.moveTo(nodeToUnlock);
  // unlockKey(swtx.getCurrentNode().getKey());
  // while (swtx.moveTo(swtx.getCurrentNode().getParentKey())) {
  // unlockKey(swtx.getCurrentNode().getKey());
  // }
  // swtx.moveTo(current);
  // }
}
