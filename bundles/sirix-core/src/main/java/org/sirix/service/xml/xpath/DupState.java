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

package org.sirix.service.xml.xpath;

/**
 * Enum represents different states of the current result sequence that helps to
 * specify, whether the result query will contain duplicates.
 */
public enum DupState {

  /** State of the HiddersMichiels - automaton. */
  MAX1 {

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupFollPreSib() {

      return DupState.GENSIB;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupAncestor() {

      return DupState.LIN;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupChild() {

      return DupState.GENSIB;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupFollPre() {

      DupState.nodup = true;
      return DupState.NO;
    }

  },

  /** State of the HiddersMichiels - automaton. */
  LIN {

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupFollPreSib() {

      return DupState.SIB;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupDesc() {

      DupState.nodup = false;
      return DupState.NO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupChild() {

      return DupState.NO;
    }

  },

  /** State of the HiddersMichiels - automaton. */
  GENSIB {

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupParent() {

      DupState.nodup = false;
      return DupState.NO;
    }
  },

  /** State of the HiddersMichiels - automaton. */
  SIB {

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupParent() {

      DupState.nodup = false;
      return DupState.NO;
    }

  },

  /** State of the HiddersMichiels - automaton. */
  NO {

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupDesc() {

      DupState.nodup = false;
      return DupState.NO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DupState updateDupParent() {

      DupState.nodup = false;
      return DupState.NO;
    }
  };

  /** Is true, if the expression is still duplicate free. */
  static boolean nodup = true;

  /**
   * Changes the state according to a child step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupChild() {

    return this;
  }

  /**
   * Changes the state according to a parent step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupParent() {

    return this;
  }

  /**
   * Changes the state according to a descendant, descendant-or-self step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupDesc() {

    return DupState.NO;
  }

  /**
   * Changes the state according to a following /preceding step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupFollPre() {

    DupState.nodup = false;
    return DupState.NO;
  }

  /**
   * Changes the state according to a following-sibling/preceding-sibling step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupFollPreSib() {

    DupState.nodup = false;
    return DupState.NO;
  }

  /**
   * Changes the state according to a ancestor step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateDupAncestor() {

    DupState.nodup = false;
    return DupState.NO;
  }

  /**
   * Changes the state according to a union step.
   * 
   * @return the updated duplicate state
   */
  public DupState updateUnion() {
    DupState.nodup = false;
    return this;
  }
}
