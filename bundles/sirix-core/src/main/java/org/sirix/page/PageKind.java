/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
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

import org.sirix.api.PageReadOnlyTrx;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new UnorderedKeyValuePage(source, pageReadTrx);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(RECORDPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page nodePage, final PageReadOnlyTrx pageReadTrx) {
      assert nodePage instanceof UnorderedKeyValuePage;
      final UnorderedKeyValuePage page = (UnorderedKeyValuePage) nodePage;
      return new UnorderedKeyValuePage(page.getPageKey(), page.getPageKind(), page.getPreviousReferenceKey(),
          pageReadTrx);
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte) 2, NamePage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new NamePage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(NAMEPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new NamePage();
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte) 3, UberPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new UberPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(UBERPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new UberPage();
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte) 4, IndirectPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new IndirectPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(INDIRECTPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new IndirectPage();
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte) 5, RevisionRootPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new RevisionRootPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(REVISIONROOTPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new RevisionRootPage();
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte) 6, PathSummaryPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx,
        final @Nonnull SerializationType type) throws IOException {
      return new PathSummaryPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final @Nonnull SerializationType type)
        throws IOException {
      sink.writeByte(PATHSUMMARYPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new PathSummaryPage();
    }
  },

  /**
   * {@link TextValuePage}.
   */
  TEXTVALUEPAGE((byte) 7, PathPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx,
        final @Nonnull SerializationType type) throws IOException {
      return new PathPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final @Nonnull SerializationType type)
        throws IOException {
      sink.writeByte(TEXTVALUEPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new PathPage();
    }
  },

  /**
   * {@link CASPage}.
   */
  CASPAGE((byte) 8, CASPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new CASPage(source, type);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException {
      sink.writeByte(CASPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new CASPage();
    }
  },

  /**
   * {@link AttributeValuePage}.
   */
  OVERFLOWPAGE((byte) 9, OverflowPage.class) {
    @Override
    @Nonnull
    Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
        throws IOException {
      return new OverflowPage(source);
    }

    @Override
    void serializePage(final DataOutput sink, final Page page, @Nonnull SerializationType type) throws IOException {
      sink.writeByte(OVERFLOWPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    public @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx) {
      return new OverflowPage();
    }
  },

  /**
   * {@link PathPage}.
   */
  PATHPAGE((byte) 10, PathPage.class) {
    @Override
    void serializePage(DataOutput sink, @Nonnull Page page, @Nonnull SerializationType type) throws IOException {
      sink.writeByte(PATHPAGE.mId);
      page.serialize(sink, type);
    }

    @Override
    Page deserializePage(DataInput source, @Nonnull PageReadOnlyTrx pageReadTrx, @Nonnull SerializationType type)
        throws IOException {
      return new PathPage(source, type);
    }

    @Override
    public @Nonnull Page getInstance(Page page, @Nonnull PageReadOnlyTrx pageReadTrx) {
      return new PathPage();
    }
  };

  /** Mapping of keys -> page */
  private static final Map<Byte, PageKind> INSTANCEFORID = new HashMap<>();

  /** Mapping of class -> page. */
  private static final Map<Class<? extends Page>, PageKind> INSTANCEFORCLASS = new HashMap<>();

  static {
    for (final PageKind page : values()) {
      INSTANCEFORID.put(page.mId, page);
      INSTANCEFORCLASS.put(page.mClass, page);
    }
  }

  /** Unique ID. */
  private final byte mId;

  /** Class. */
  private final Class<? extends Page> mClass;

  /**
   * Constructor.
   *
   * @param pId unique identifier
   * @param clazz class
   */
  PageKind(final byte pId, final Class<? extends Page> clazz) {
    mId = pId;
    mClass = clazz;
  }

  /**
   * Get the unique page ID.
   *
   * @return unique page ID
   */
  public byte getID() {
    return mId;
  }

  /**
   * Serialize page.
   *
   * @param sink {@link DataInput} instance
   * @param page {@link Page} implementation
   */
  abstract void serializePage(final DataOutput sink, final Page page, final SerializationType type) throws IOException;

  /**
   * Deserialize page.
   *
   * @param source {@link DataInput} instance
   * @param pageReadTrx implementing {@link PageReadOnlyTrx} instance
   * @return page instance implementing the {@link Page} interface
   */
  abstract Page deserializePage(final DataInput source, final PageReadOnlyTrx pageReadTrx, final SerializationType type)
      throws IOException;

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
  public static @Nonnull PageKind getKind(final Class<? extends Page> clazz) {
    final PageKind page = INSTANCEFORCLASS.get(clazz);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }

  /**
   * New page instance.
   *
   * @param page instance of class which implements {@link Page}
   * @param pageReadTrx instance of class which implements {@link PageReadOnlyTrx}
   * @return new page instance
   */
  public abstract @Nonnull Page getInstance(final Page page, final PageReadOnlyTrx pageReadTrx);
}
