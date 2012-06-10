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

import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.page.interfaces.IPage;

/**
 * Persists pages on secondary storage.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PagePersistenter {

  /**
   * All Page types.
   */
  private enum EPage {
    /**
     * {@link NodePage}.
     */
    NODEPAGE((byte)1, NodePage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new NodePage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(NODEPAGE.mId);
        serialize(pSink, pPage);
      }
    },

    /**
     * {@link NamePage}.
     */
    NAMEPAGE((byte)2, NamePage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new NamePage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(NAMEPAGE.mId);
        serialize(pSink, pPage);
      }
    },

    /** 
     * {@link UberPage}.
     */
    UBERPAGE((byte)3, UberPage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new UberPage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(UBERPAGE.mId);
        serialize(pSink, pPage);
      }
    },

    /**
     * {@link IndirectPage}.
     */
    INDIRECTPAGE((byte)4, IndirectPage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new IndirectPage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(INDIRECTPAGE.mId);
        serialize(pSink, pPage);
      }
    },

    /**
     * {@link RevisionRootPage}.
     */
    REVISIONROOTPAGE((byte)5, RevisionRootPage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new RevisionRootPage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(REVISIONROOTPAGE.mId);
        serialize(pSink, pPage);
      }
    },
    
    /**
     * {@link MetaPage}.
     */
    METAPAGE((byte)6, MetaPage.class) {
      @Override
      IPage deserializePage(final ITTSource pSource) {
        return new MetaPage(pSource);
      }

      @Override
      void serializePage(final ITTSink pSink, final IPage pPage) {
        pSink.writeByte(METAPAGE.mId);
        serialize(pSink, pPage);
      }
    };
    
    private static final void serialize(final ITTSink pSink, final IPage pPage) {
      pPage.serialize(pSink);
    }

    /** Mapping of keys -> page */
    private static final Map<Byte, EPage> INSTANCEFORID = new HashMap<>();

    /** Mapping of class -> page. */
    private static final Map<Class<? extends IPage>, EPage> INSTANCEFORCLASS = new HashMap<>();

    static {
      for (final EPage node : values()) {
        INSTANCEFORID.put(node.mId, node);
        INSTANCEFORCLASS.put(node.mClass, node);
      }
    }

    /** Unique ID. */
    private final byte mId;

    /** Class. */
    private final Class<? extends IPage> mClass;

    /**
     * Constructor.
     * 
     * @param pId
     *          unique identifier
     * @param pClass
     *          class
     */
    EPage(final byte pId, final Class<? extends IPage> pClass) {
      mId = pId;
      mClass = pClass;
    }

    /**
     * Serialize page.
     * 
     * @param pSink
     *          {@link ITTSink} implementation
     * @param pPage
     *          {@link IPage} implementation
     */
    abstract void serializePage(@Nonnull final ITTSink pSink, @Nonnull final IPage pPage);

    /**
     * Deserialize page.
     * 
     * @param pSource
     *          {@link ITTSource} implementation
     * @return page instance implementing the {@link IPage} interface
     */
    abstract IPage deserializePage(@Nonnull final ITTSource pSource);

    /**
     * Public method to get the related page based on the identifier.
     * 
     * @param pId
     *          the identifier for the page
     * @return the related page
     */
    public static EPage getKind(final byte pId) {
      return INSTANCEFORID.get(pId);
    }

    /**
     * Public method to get the related page based on the class.
     * 
     * @param pClass
     *          the class for the page
     * @return the related page
     */
    public static EPage getKind(final Class<? extends IPage> pClass) {
      return INSTANCEFORCLASS.get(pClass);
    }
  }

  /**
   * Deserialize page.
   * 
   * @param pSource
   *          source to read from
   * @return the created page
   */
  public static IPage deserializePage(@Nonnull final ITTSource pSource) {
    return EPage.getKind(pSource.readByte()).deserializePage(pSource);
  }

  /**
   * Serialize page.
   * 
   * @param pSink
   *          output sink
   * @param pPage
   *          the page to serialize
   */
  public static void serializePage(@Nonnull final ITTSink pSink, @Nonnull final IPage pPage) {
    EPage.getKind(pPage.getClass()).serializePage(pSink, pPage);
  }
}
