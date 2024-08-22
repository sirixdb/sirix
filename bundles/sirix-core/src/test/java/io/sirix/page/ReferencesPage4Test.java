/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.page;

import org.junit.jupiter.api.Test;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.page.interfaces.PageFragmentKey;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the {@link ReferencesPage4}.
 *
 * @author Johannes Lichtenberger
 */
public final class ReferencesPage4Test {

	@Test
	public void testCloneConstructor() {
		final var referencesPage4 = new ReferencesPage4();

		final var pageReference = referencesPage4.getOrCreateReference(0);
		assert pageReference != null;
		pageReference.setLogKey(5);

		final List<PageFragmentKey> pageFragmentKeys = List.of(new PageFragmentKeyImpl(1, 200),
				new PageFragmentKeyImpl(2, 763));

		pageReference.setPageFragments(pageFragmentKeys);

		final var newReferencesPage4 = new ReferencesPage4(referencesPage4);

		assertNotSame(referencesPage4, newReferencesPage4);

		final var copiedPageReference = newReferencesPage4.getOrCreateReference(0);

		assert copiedPageReference != null;
		assertEquals(pageReference.getLogKey(), copiedPageReference.getLogKey());

		final List<PageFragmentKey> copiedPageFragmentKeys = copiedPageReference.getPageFragments();

		assertEquals(copiedPageFragmentKeys.size(), 2);

		assertEquals(pageFragmentKeys.get(0).revision(), copiedPageFragmentKeys.get(0).revision());
		assertEquals(pageFragmentKeys.get(1).revision(), copiedPageFragmentKeys.get(1).revision());

		assertEquals(pageFragmentKeys.get(0).key(), copiedPageFragmentKeys.get(0).key());
		assertEquals(pageFragmentKeys.get(1).key(), copiedPageFragmentKeys.get(1).key());
	}
}
