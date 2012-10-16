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

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.sirix.page.interfaces.Page;

/**
 * All Page types.
 */
public enum EPage {
  /**
   * {@link NodePage}.
   */
  NODEPAGE((byte)1, NodePage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new NodePage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(NODEPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      assert pPage instanceof NodePage;
      final NodePage page = (NodePage)pPage;
      return new NodePage(page.getNodePageKey(), page.getRevision());
    }
  },

  /**
   * {@link NamePage}.
   */
  NAMEPAGE((byte)2, NamePage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new NamePage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(NAMEPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new NamePage(pPage.getRevision());
    }
  },

  /**
   * {@link UberPage}.
   */
  UBERPAGE((byte)3, UberPage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new UberPage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(UBERPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new UberPage();
    }
  },

  /**
   * {@link IndirectPage}.
   */
  INDIRECTPAGE((byte)4, IndirectPage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new IndirectPage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(INDIRECTPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new IndirectPage(pPage.getRevision());
    }
  },

  /**
   * {@link RevisionRootPage}.
   */
  REVISIONROOTPAGE((byte)5, RevisionRootPage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new RevisionRootPage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(REVISIONROOTPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new RevisionRootPage();
    }
  },

  /**
   * {@link PathSummaryPage}.
   */
  PATHSUMMARYPAGE((byte)6, PathSummaryPage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new PathSummaryPage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(PATHSUMMARYPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new PathSummaryPage(pPage.getRevision());
    }
  }, 
  
  /**
   * {@link ValuePage}.
   */
  VALUEPAGE((byte)7, ValuePage.class) {
    @Override
    @Nonnull
    Page deserializePage(@Nonnull final ByteArrayDataInput pSource) {
      return new ValuePage(pSource);
    }

    @Override
    void
      serializePage(@Nonnull final ByteArrayDataOutput pSink, @Nonnull final Page pPage) {
      pSink.writeByte(VALUEPAGE.mId);
      pPage.serialize(pSink);
    }

    @Override
    public @Nonnull
    Page getInstance(@Nonnull final Page pPage) {
      return new ValuePage(pPage.getRevision());
    }
  };

  /** Mapping of keys -> page */
  private static final Map<Byte, EPage> INSTANCEFORID = new HashMap<>();

  /** Mapping of class -> page. */
  private static final Map<Class<? extends Page>, EPage> INSTANCEFORCLASS =
    new HashMap<>();

  static {
    for (final EPage node : values()) {
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
   * @param pClass
   *          class
   */
  EPage(final byte pId, final Class<? extends Page> pClass) {
    mId = pId;
    mClass = pClass;
  }

  /**
   * Serialize page.
   * 
   * @param pSink
   *          {@link ITTSink} implementation
   * @param pPage
   *          {@link Page} implementation
   */
  abstract void serializePage(@Nonnull final ByteArrayDataOutput pSink,
    @Nonnull final Page pPage);

  /**
   * Deserialize page.
   * 
   * @param pSource
   *          {@link ITTSource} implementation
   * @return page instance implementing the {@link Page} interface
   */
  abstract Page deserializePage(@Nonnull final ByteArrayDataInput pSource);

  /**
   * Public method to get the related page based on the identifier.
   * 
   * @param pId
   *          the identifier for the page
   * @return the related page
   */
  public static EPage getKind(final byte pId) {
    final EPage page = INSTANCEFORID.get(pId);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }

  /**
   * Public method to get the related page based on the class.
   * 
   * @param pClass
   *          the class for the page
   * @return the related page
   */
  public static @Nonnull
  EPage getKind(@Nonnull final Class<? extends Page> pClass) {
    final EPage page = INSTANCEFORCLASS.get(pClass);
    if (page == null) {
      throw new IllegalStateException();
    }
    return page;
  }

  /**
   * New page instance.
   * 
   * @param pPage
   *          {@link Page} implementation
   * @return new page instance
   */
  public abstract @Nonnull
  Page getInstance(@Nonnull final Page pPage);
}
