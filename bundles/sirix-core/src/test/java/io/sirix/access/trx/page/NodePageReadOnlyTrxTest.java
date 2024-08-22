package io.sirix.access.trx.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.InternalResourceSession;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.cache.BufferManager;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.page.UberPage;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Assert;
import org.junit.Test;
import io.sirix.io.Reader;
import io.sirix.settings.Constants;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class NodePageReadOnlyTrxTest {

	@Test
	public void testPageKey() {
		final InternalResourceSession<?, ?> resourceManagerMock = createResourceManagerMock();

		final var trx = new NodePageReadOnlyTrx(1, resourceManagerMock, new UberPage(), 0, mock(Reader.class),
				mock(BufferManager.class), mock(RevisionRootPageReader.class), mock(TransactionIntentLog.class));

		assertEquals(0, trx.pageKey(1, IndexType.DOCUMENT));
		assertEquals(1023 / Constants.NDP_NODE_COUNT, trx.pageKey(1023, IndexType.DOCUMENT));
		assertEquals(1024 / Constants.NDP_NODE_COUNT, trx.pageKey(1024, IndexType.DOCUMENT));
	}

	@Test
	public void testRecordPageOffset() {
		Assert.assertEquals(1, PageReadOnlyTrx.recordPageOffset(1));
		assertEquals(Constants.NDP_NODE_COUNT - 1, PageReadOnlyTrx.recordPageOffset(1023));
	}

	@NonNull
	private InternalResourceSession<?, ?> createResourceManagerMock() {
		final var resourceManagerMock = mock(InternalResourceSession.class);
		when(resourceManagerMock.getResourceConfig()).thenReturn(new ResourceConfiguration.Builder("foobar").build());
		return resourceManagerMock;
	}
}
