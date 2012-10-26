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
package org.sirix.page;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.page.interfaces.Page;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * All Page types.
 */
public enum PageKind {
	/**
	 * {@link NodePage}.
	 */
	NODEPAGE((byte) 1, RecordPage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new RecordPage(source, pageReadTrx);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(NODEPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page nodePage,
				final @Nonnull PageReadTrx pageReadTrx) {
			assert nodePage instanceof RecordPage;
			final RecordPage page = (RecordPage) nodePage;
			return new RecordPage(page.getNodePageKey(), page.getRevision(),
					pageReadTrx);
		}
	},

	/**
	 * {@link NamePage}.
	 */
	NAMEPAGE((byte) 2, NamePage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new NamePage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(NAMEPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page page,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new NamePage(page.getRevision());
		}
	},

	/**
	 * {@link UberPage}.
	 */
	UBERPAGE((byte) 3, UberPage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new UberPage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(UBERPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page page,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new UberPage();
		}
	},

	/**
	 * {@link IndirectPage}.
	 */
	INDIRECTPAGE((byte) 4, IndirectPage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new IndirectPage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(INDIRECTPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page page,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new IndirectPage(page.getRevision());
		}
	},

	/**
	 * {@link RevisionRootPage}.
	 */
	REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new RevisionRootPage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(REVISIONROOTPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page page,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new RevisionRootPage();
		}
	},

	/**
	 * {@link PathSummaryPage}.
	 */
	PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new PathSummaryPage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(PATHSUMMARYPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page page,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new PathSummaryPage(page.getRevision());
		}
	},

	/**
	 * {@link ValuePage}.
	 */
	VALUEPAGE((byte) 7, ValuePage.class) {
		@Override
		@Nonnull
		Page deserializePage(@Nonnull final ByteArrayDataInput source,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new ValuePage(source);
		}

		@Override
		void serializePage(@Nonnull final ByteArrayDataOutput sink,
				@Nonnull final Page page) {
			sink.writeByte(VALUEPAGE.mId);
			page.serialize(sink);
		}

		@Override
		public @Nonnull
		Page getInstance(@Nonnull final Page pPage,
				final @Nonnull PageReadTrx pageReadTrx) {
			return new ValuePage(pPage.getRevision());
		}
	};

	/** Mapping of keys -> page */
	private static final Map<Byte, PageKind> INSTANCEFORID = new HashMap<>();

	/** Mapping of class -> page. */
	private static final Map<Class<? extends Page>, PageKind> INSTANCEFORCLASS = new HashMap<>();

	static {
		for (final PageKind node : values()) {
			INSTANCEFORID.put(node.mId, node);
			INSTANCEFORCLASS.put(node.mClass, node);
		}
	}

	/** Unique ID. */
	private final byte mId;

	/** Class. */
	private final Class<? extends Page> mClass;

	/**
	 * Constructor.
	 * 
	 * @param pId
	 *          unique identifier
	 * @param clazz
	 *          class
	 */
	PageKind(final byte pId, final Class<? extends Page> clazz) {
		mId = pId;
		mClass = clazz;
	}

	/**
	 * Serialize page.
	 * 
	 * @param sink
	 *          {@link ITTSink} implementation
	 * @param page
	 *          {@link Page} implementation
	 */
	abstract void serializePage(@Nonnull final ByteArrayDataOutput sink,
			@Nonnull final Page page);

	/**
	 * Deserialize page.
	 * 
	 * @param source
	 *          {@link ITTSource} implementation
	 * @param resourceConfig
	 *          resource configration
	 * @return page instance implementing the {@link Page} interface
	 */
	abstract Page deserializePage(@Nonnull final ByteArrayDataInput source,
			final @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Public method to get the related page based on the identifier.
	 * 
	 * @param id
	 *          the identifier for the page
	 * @return the related page
	 */
	public static PageKind getKind(final byte id) {
		final PageKind page = INSTANCEFORID.get(id);
		if (page == null) {
			throw new IllegalStateException();
		}
		return page;
	}

	/**
	 * Public method to get the related page based on the class.
	 * 
	 * @param clazz
	 *          the class for the page
	 * @return the related page
	 */
	public static @Nonnull
	PageKind getKind(@Nonnull final Class<? extends Page> clazz) {
		final PageKind page = INSTANCEFORCLASS.get(clazz);
		if (page == null) {
			throw new IllegalStateException();
		}
		return page;
	}

	/**
	 * New page instance.
	 * 
	 * @param page
	 *          {@link Page} implementation
	 * @return new page instance
	 */
	public abstract @Nonnull
	Page getInstance(@Nonnull final Page page,
			final @Nonnull PageReadTrx pageReadTrx);
}
