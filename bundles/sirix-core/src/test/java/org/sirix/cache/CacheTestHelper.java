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
package org.sirix.cache;

import org.junit.Test;
import org.sirix.Holder;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * Helper class for testing the cache.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class CacheTestHelper {

	/** Page reading transaction. */
	private static PageReadTrx PAGE_READ_TRX;

	/** Unordered record pages. */
	protected static UnorderedKeyValuePage[][] PAGES;

	/**
	 * Setup the cache.
	 * 
	 * @param cache
	 *          cache to fill
	 * @throws SirixException
	 *           if setting up Sirix session fails
	 */
	public static void setUp(
			final Cache<Long, RecordPageContainer<UnorderedKeyValuePage>> cache)
			throws SirixException {
		PAGE_READ_TRX = Holder.generateSession().getSession().beginPageReadTrx();
		PAGES = new UnorderedKeyValuePage[LRUCache.CACHE_CAPACITY + 1][ResourceConfiguration.VERSIONSTORESTORE + 1];
		for (int i = 0; i < PAGES.length; i++) {
			final UnorderedKeyValuePage page = new UnorderedKeyValuePage(i, 0,
					PAGE_READ_TRX);
			final UnorderedKeyValuePage[] revs = new UnorderedKeyValuePage[ResourceConfiguration.VERSIONSTORESTORE];

			for (int j = 0; j < ResourceConfiguration.VERSIONSTORESTORE; j++) {
				PAGES[i][j + 1] = new UnorderedKeyValuePage(i, 0, PAGE_READ_TRX);
				revs[j] = PAGES[i][j + 1];
			}
			PAGES[i][0] = page;
			cache.put((long) i, new RecordPageContainer<>(page));
		}
	}

	@Test
	public void dummy() {
		// Only for dummy purposes.
	}

}
