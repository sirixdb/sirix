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

package org.treetank.access;

public class SynchWriteTransaction/* extends WriteTransaction */{

  // private LockManager lock;
  //
  // protected SynchWriteTransaction(long mTransactionID, Session mSessionState,
  // WriteTransactionState mTransactionState, int maxNodeCount, int maxTime) throws AbsTTException {
  // super(mTransactionID, mSessionState, mTransactionState, maxNodeCount, maxTime);
  // lock = LockManager.getLockManager();
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertElementAsFirstChild(final QName mQname) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return super.insertElementAsFirstChild(mQname);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertElementAsRightSibling(final QName mQname) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return super.insertElementAsRightSibling(mQname);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertTextAsFirstChild(final String mValueAsString) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return super.insertTextAsFirstChild(mValueAsString);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertTextAsRightSibling(final String mValueAsString) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return insertTextAsRightSibling(mValueAsString);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertAttribute(final QName mQname, final String mValueAsString)
  // throws AbsTTException {
  // if (getCurrentNode() instanceof ElementNode) {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return super.insertAttribute(mQname, mValueAsString);
  // } else {
  // throw new TTUsageException("Insert is not allowed if current node is not an ElementNode!");
  // }
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized long insertNamespace(final QName paramQName) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // return super.insertNamespace(paramQName);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized void remove() throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // super.remove();
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized void setQName(final QName paramName) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // super.setQName(paramName);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized void setURI(final String mUri) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // super.setURI(mUri);
  // }
  //
  // /**
  // * {@inheritDoc}
  // */
  // @Override
  // public synchronized void setValue(final String mValue) throws AbsTTException {
  // lock.getWritePermission(getCurrentNode().getKey(), this);
  // super.setValue(mValue);
  // }
}
