/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.page;

import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.page.interfaces.Page;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * All Page types.
 */
public enum PageKind {
  /**
   * {@link UnorderedKeyValuePage}.
   */
  RECORDPAGE((byte) 1, UnorderedKeyValuePage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new UnorderedKeyValuePage(source, pageReadTrx);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(RECORDPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page nodePage, final PageReadOnlyTrx pageReadTrx) {
      assert nodePage instanceof UnorderedKeyValuePage;
      final UnorderedKeyValuePage page = (UnorderedKeyValuePage) nodePage;
      return new UnorderedKeyValuePage(page.getPageKey(), page.getIndexType(), pageReadTrx);
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new NamePage(source, type);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(NAMEPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new NamePage();
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new UberPage(source);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(UBERPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new UberPage();
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte) 4, IndirectPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new IndirectPage(source, type);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(INDIRECTPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new IndirectPage();
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new RevisionRootPage(source, type);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(REVISIONROOTPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new RevisionRootPage();
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final @NonNull SerializationType type) {
      return new PathSummaryPage(source, type);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final @NonNull SerializationType type) {
      sink.writeByte(PATHSUMMARYPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new PathSummaryPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  HASHED_KEY_VALUE_PAGE((byte) 7, HashedKeyValuePage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        @NonNull final SerializationType type) {
      return new HashedKeyValuePage(source, pageReadTrx);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final @NonNull SerializationType type) {
      sink.writeByte(HASHED_KEY_VALUE_PAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page keyValuePage, final PageReadOnlyTrx pageReadTrx) {
      assert keyValuePage instanceof HashedKeyValuePage;
      final HashedKeyValuePage page = (HashedKeyValuePage) keyValuePage;
      return new UnorderedKeyValuePage(page.getPageKey(), page.getIndexType(), pageReadTrx);
    }
  },

  /**
   * {@link CASPage}.
   */
  CASPAGE((byte) 8, CASPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new CASPage(source, type);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type) {
      sink.writeByte(CASPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new CASPage();
    }
  },

  /**
   * {@link OverflowPage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    @NonNull Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
        final SerializationType type) {
      return new OverflowPage(source);
    }

    @Override
    void serializePage(final Bytes<ByteBuffer> sink, final Page page, @NonNull SerializationType type) {
      sink.writeByte(OVERFLOWPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    public @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new OverflowPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {
    @Override
    void serializePage(Bytes<ByteBuffer> sink, @NonNull Page page, @NonNull SerializationType type) {
      sink.writeByte(PATHPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    Page deserializePage(Bytes<ByteBuffer> source, @NonNull PageReadOnlyTrx pageReadTrx,
        @NonNull SerializationType type) {
      return new PathPage(source, type);
    }

    @Override
    public @NonNull Page getInstance(Page page, @NonNull PageReadOnlyTrx pageReadTrx) {
      return new PathPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  DEWEYIDPAGE((byte) 11, DeweyIDPage.class) {
    @Override
    void serializePage(Bytes<ByteBuffer> sink, @NonNull Page page, @NonNull SerializationType type) {
      sink.writeByte(DEWEYIDPAGE.id);
      page.serialize(sink, type);
    }

    @Override
    Page deserializePage(Bytes<ByteBuffer> source, @NonNull PageReadOnlyTrx pageReadTrx,
        @NonNull SerializationType type) {
      return new DeweyIDPage(source, type);
    }

    @Override
    public @NonNull Page getInstance(Page page, @NonNull PageReadOnlyTrx pageReadTrx) {
      return new DeweyIDPage();
    }
  };

  /**
   * Mapping of keys -> page
   */
  private static final Map<Byte, PageKind> INSTANCEFORID = new HashMap<>();

  /**
   * Mapping of class -> page.
   */
  private static final Map<Class<? extends Page>, PageKind> INSTANCEFORCLASS = new HashMap<>();

  static {
    for (final PageKind page : values()) {
      INSTANCEFORID.put(page.id, page);
      INSTANCEFORCLASS.put(page.clazz, page);
    }
  }

  /**
   * Unique ID.
   */
  private final byte id;

  /**
   * Class.
   */
  private final Class<? extends Page> clazz;

  /**
   * Constructor.
   *
   * @param id    unique identifier
   * @param clazz class
   */
  PageKind(final byte id, final Class<? extends Page> clazz) {
    this.id = id;
    this.clazz = clazz;
  }

  /**
   * Get the unique page ID.
   *
   * @return unique page ID
   */
  public byte getID() {
    return id;
  }

  /**
   * Serialize page.
   *
   * @param sink {@link Bytes<ByteBuffer>} instance
   * @param page {@link Page} implementation
   */
  abstract void serializePage(final Bytes<ByteBuffer> sink, final Page page, final SerializationType type);

  /**
   * Deserialize page.
   *
   * @param source      {@link Bytes<ByteBuffer>} instance
   * @param pageReadTrx implementing {@link PageReadOnlyTrx} instance
   * @return page instance implementing the {@link Page} interface
   */
  abstract Page deserializePage(final Bytes<ByteBuffer> source, final PageReadOnlyTrx pageReadTrx,
      final SerializationType type);

  /**
   * Public method to get the related page based on the identifier.
   *
   * @param id the identifier for the page
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
   * @param clazz the class for the page
   * @return the related page
   */
  public static @NonNull PageKind getKind(final Class<? extends Page> clazz) {
    final PageKind page = INSTANCEFORCLASS.get(clazz);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }

  /**
   * New page instance.
   *
   * @param page        instance of class which implements {@link Page}
   * @param pageReadTrx instance of class which implements {@link PageReadOnlyTrx}
   * @return new page instance
   */
  public abstract @NonNull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx);
}
