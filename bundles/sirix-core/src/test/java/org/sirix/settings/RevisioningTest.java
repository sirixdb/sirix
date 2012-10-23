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

package org.sirix.settings;

import org.junit.After;
import org.junit.Before;
import org.sirix.TestHelper;
import org.sirix.exception.SirixException;

public class RevisioningTest {

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
	}

	@After
	public void tearDown() throws SirixException {
		TestHelper.closeEverything();
	}

	// // @Test(expected = AssertionError.class)
	// @Test
	// public void testFulldumpCombinePages() {
	// final NodePage[] pages = new NodePage[1];
	// pages[0] = getNodePage(1, 0, 128, 0);
	// // pages[1] = getNodePage(0, 0, 128, 0);
	//
	// final NodePage page = ERevisioning.FULL.combineNodePages(pages,
	// ResourceConfiguration.VERSIONSTORESTORE);
	//
	// for (int j = 0; j < page.getNodes().length; j++) {
	// assertEquals(pages[0].getNode(j), page.getNode(j));
	// }
	//
	// }
	//
	// @Test
	// public void testDifferentialCombinePages() {
	// final NodePage[] pages = prepareNormal(2);
	// final NodePage page =
	// ERevisioning.DIFFERENTIAL.combineNodePages(pages,
	// ResourceConfiguration.VERSIONSTORESTORE);
	//
	// for (int j = 0; j < 32; j++) {
	// assertEquals(pages[0].getNode(j), page.getNode(j));
	// }
	// for (int j = 32; j < page.getNodes().length; j++) {
	// assertEquals(pages[1].getNode(j), page.getNode(j));
	// }
	//
	// }
	//
	// @Test
	// public void testIncrementalCombinePages() {
	// final NodePage[] pages = prepareNormal(4);
	// final NodePage page =
	// ERevisioning.INCREMENTAL.combineNodePages(pages, 4);
	// checkCombined(pages, page);
	// }
	//
	// private static NodePage[] prepareNormal(final int length) {
	// final NodePage[] pages = new NodePage[length];
	// pages[pages.length - 1] = getNodePage(0, 0, 128, 0);
	// for (int i = 0; i < pages.length - 1; i++) {
	// pages[i] = getNodePage(pages.length - i - 1, i * 32, (i * 32) + 32, 0);
	// }
	// return pages;
	// }
	//
	// // private static NodePage[] prepareOverlapping(final int length) {
	// // final NodePage[] pages = new NodePage[length];
	// // final int[] borders = new int[4];
	// // pages[pages.length - 1] = getNodePage(0, 0, 128);
	// // for (int i = 0; i < pages.length - 1; i++) {
	// // borders[i] = random.nextInt(32) + ((i) * 32);
	// // pages[i] = getNodePage(pages.length - i, borders[i], (i * 32) + 32);
	// // }
	// // return pages;
	// //
	// // }
	//
	// private static void checkCombined(final NodePage[] toCheck, final NodePage
	// page) {
	// for (int i = 0; i < 4; i++) {
	// for (int j = i * 32; j < (i * 32) + 32; j++) {
	// assertEquals(toCheck[i].getNode(j), page.getNode(j));
	// }
	// }
	// }

}
