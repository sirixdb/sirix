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

package org.treetank.service.xml.xpath.filter;

// /*
// * Copyright (c) 2008, Tina Scherer (Master Thesis), University of Konstanz
// *
// * Permission to use, copy, modify, and/or distribute this software for any
// * purpose with or without fee is hereby granted, provided that the above
// * copyright notice and this permission notice appear in all copies.
// *
// * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
// * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
// * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
// * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
// * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
// * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
// * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
// *
// * $Id: PosFilter.java 4238 2008-07-03 13:32:30Z scherer $
// */
//
// package org.treetank.xpath.filter;
//
// import org.treetank.api.IAxis;
// import org.treetank.api.IReadTransaction;
// import org.treetank.axislayer.AbstractAxis;
//
// /**
// * @author Tina Scherer
// */
// public class PosFilter extends AbstractAxis implements IAxis {
//
// private final int mExpectedPos;
//
// /** The position of the current item. */
// private int mPosCount;
//
// /**
// * Constructor. Initializes the internal state.
// *
// * @param rtx
// * Exclusive (immutable) trx to iterate with.
// * @param expectedPos
// * he expected position
// */
// public PosFilter(final IReadTransaction rtx, final int expectedPos) {
//
// super(rtx);
// mExpectedPos = expectedPos;
// mPosCount = 0;
// }
//
// /**
// * {@inheritDoc}
// */
// @Override
// public final void reset(final long nodeKey) {
//
// super.reset(nodeKey);
// mPosCount = 0;
// }
//
// /**
// * {@inheritDoc}
// */
// public final boolean hasNext() {
//
// resetToLastKey();
//
// // a predicate has to evaluate to true only once.
// if (mExpectedPos == ++mPosCount) {
// return true;
// }
//
// resetToStartKey();
// return false;
//
// }
//
// }
