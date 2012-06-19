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
package org.sirix.access.conf;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import java.io.File;

import javax.annotation.Nonnull;

import org.sirix.access.NodeWriteTrx.EHashKind;
import org.sirix.access.Session;
import org.sirix.access.conf.ResourceConfiguration.Builder.EConsistency;
import org.sirix.io.EStorage;
import org.sirix.settings.ERevisioning;

/**
 * <h1>ResourceConfiguration</h1>
 * 
 * <p>
 * Holds the settings for a resource which acts as a base for session that can not change. This includes all
 * settings which are persistent. Each {@link ResourceConfiguration} is furthermore bound to one fixed
 * database denoted by a related {@link DatabaseConfiguration}.
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 */
public final class ResourceConfiguration implements IConfigureSerializable {

  /** For serialization. */
  private static final long serialVersionUID = 1790483717305421672L;

  /**
   * Paths for a {@link Session}. Each resource has the same folder layout.
   */
  public enum Paths {

    /** Folder for storage of data. */
    Data(new File("data"), true),
    /** Folder for transaction log. */
    TransactionLog(new File("log"), true),
    /** File to store the resource settings. */
    ConfigBinary(new File("ressetting.obj"), false);

    /** Location of the file. */
    private final File mFile;

    /** Is the location a folder or no? */
    private final boolean mIsFolder;

    /**
     * Constructor.
     * 
     * @param pFile
     *          to be set
     * @param pIsFolder
     *          to be set.
     */
    private Paths(final File pFile, final boolean pIsFolder) {
      mFile = checkNotNull(pFile);
      mIsFolder = checkNotNull(pIsFolder);
    }

    /**
     * Getting the file for the kind.
     * 
     * @return the file to the kind
     */
    public File getFile() {
      return mFile;
    }

    /**
     * Check if file is denoted as folder or not.
     * 
     * @return boolean if file is folder
     */
    public boolean isFolder() {
      return mIsFolder;
    }

    /**
     * Checking a structure in a folder to be equal with the data in this
     * enum.
     * 
     * @param pFile
     *          to be checked
     * @return -1 if less folders are there, 0 if the structure is equal to
     *         the one expected, 1 if the structure has more folders
     */
    public static int compareStructure(final File pFile) {
      int existing = 0;
      for (final Paths paths : values()) {
        final File currentFile = new File(pFile, paths.getFile().getName());
        if (currentFile.exists()) {
          existing++;
        }
      }
      return existing - values().length;
    }
  }

  // FIXED STANDARD FIELDS
  /** Standard storage. */
  public static final EStorage STORAGE = EStorage.File;
  /** Standard Versioning Approach. */
  public static final ERevisioning VERSIONING = ERevisioning.INCREMENTAL;
  /** Type of hashing. */
  public static final EHashKind HASHKIND = EHashKind.Rolling;
  /** Versions to restore. */
  public static final int VERSIONSTORESTORE = 4;
  /** Folder for tmp-database. */
  public static final String INTRINSICTEMP = "tmp";
  // END FIXED STANDARD FIELDS

  // MEMBERS FOR FIXED FIELDS
  /** Type of Storage (File, BerkeleyDB). */
  public final EStorage mType;

  /** Kind of revisioning (Full, Incremental, Differential). */
  public final ERevisioning mRevisionKind;

  /** Kind of integrity hash (rolling, postorder). */
  public final EHashKind mHashKind;

  /** Number of revisions to restore a complete set of data. */
  public final int mRevisionsToRestore;

  /** Determines consistency level. */
  public final EConsistency mConsistency;

  /** Path for the resource to be associated. */
  public final File mPath;
  // END MEMBERS FOR FIXED FIELDS

  /** DatabaseConfiguration for this {@link ResourceConfiguration}. */
  public final DatabaseConfiguration mDBConfig;

  /** Determines if text-compression should be used or not (default is true). */
  private boolean mCompression;

  /**
   * Convenience constructor using the standard settings.
   * 
   * @param pBuilder
   *          {@link Builder} reference
   */
  private ResourceConfiguration(@Nonnull final ResourceConfiguration.Builder pBuilder) {
    mType = pBuilder.mType;
    mRevisionKind = pBuilder.mRevisionKind;
    mHashKind = pBuilder.mHashKind;
    mRevisionsToRestore = pBuilder.mRevisionsToRestore;
    mDBConfig = pBuilder.mDBConfig;
    mCompression = pBuilder.mCompression;
    mConsistency = pBuilder.mConsistency;
    mPath =
      new File(new File(mDBConfig.getFile(), DatabaseConfiguration.Paths.Data.getFile().getName()),
        pBuilder.mResource);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mType, mRevisionKind, mHashKind, mPath, mDBConfig);
  }

  @Override
  public final boolean equals(final Object pObj) {
    if (this == pObj) {
      return true;
    }
    if (pObj instanceof ResourceConfiguration) {
      final ResourceConfiguration other = (ResourceConfiguration)pObj;
      return Objects.equal(mType, other.mType) && Objects.equal(mRevisionKind, other.mRevisionKind)
        && Objects.equal(mHashKind, other.mHashKind) && Objects.equal(mPath, other.mPath)
        && Objects.equal(mDBConfig, other.mDBConfig);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("Resource", mPath).add("Type", mType).add("Revision",
      mRevisionKind).add("HashKind", mHashKind).toString();
  }

  /**
   * Get resource.
   * 
   * @return resource
   */
  public File getResource() {
    return mPath;
  }

  @Override
  public File getConfigFile() {
    return new File(mPath, Paths.ConfigBinary.getFile().getName());
  }

  /**
   * Determines if compression is enabled or not.
   * 
   * @return {@code true} if it's enabled, {@code false} otherwise
   */
  public boolean isCompression() {
    return mCompression;
  }

  /**
   * Builder class for generating new {@link ResourceConfiguration} instance.
   */
  public static final class Builder {

    /** Consistency level. */
    public enum EConsistency {
      /**
       * Eventual consistency (currently descendantCount and hashvalues for the
       * first revision (0) are computed after the first commit().
       */
      EVENTUAL,

      /**
       * All modifications are done on the fly (descendantCount and hashes are
       * adapted right after an edit-operation has modified something).
       */
      FULL
    }

    /** Type of Storage (File, Berkeley). */
    private EStorage mType = STORAGE;

    /** Kind of revisioning (Incremental, Differential). */
    private ERevisioning mRevisionKind = VERSIONING;

    /** Kind of integrity hash (rolling, postorder). */
    private EHashKind mHashKind = HASHKIND;

    /** Number of revisions to restore a complete set of data. */
    private int mRevisionsToRestore = VERSIONSTORESTORE;

    /** Resource for the this session. */
    private final String mResource;

    /** Resource for the this session. */
    private final DatabaseConfiguration mDBConfig;

    /** Determines if text-compression should be used or not (default is true). */
    private boolean mCompression = true;

    /** Determines consistency level. */
    private EConsistency mConsistency = EConsistency.FULL;

    /**
     * Constructor, setting the mandatory fields.
     * 
     * @param pResource
     *          the name of the resource, must to be set.
     * @param pConfig
     *          the related {@link DatabaseConfiguration}, must to be set.
     */
    public Builder(@Nonnull final String pResource, @Nonnull final DatabaseConfiguration pConfig) {
      mResource = checkNotNull(pResource);
      mDBConfig = checkNotNull(pConfig);
    }

    /**
     * Setter for mType.
     * 
     * @param pType
     *          to be set
     * @return reference to the builder object
     */
    public Builder setType(@Nonnull final EStorage pType) {
      mType = checkNotNull(pType);
      return this;
    }

    /**
     * Setter for mRevision.
     * 
     * @param pRev
     *          to be set
     * @return reference to the builder object
     */
    public Builder setRevisionKind(final ERevisioning pRevKind) {
      mRevisionKind = checkNotNull(pRevKind);
      return this;
    }

    /**
     * Setter for mHashKind.
     * 
     * @param pHash
     *          to be set
     * @return reference to the builder object
     */
    public Builder setHashKind(@Nonnull final EHashKind pHash) {
      mHashKind = checkNotNull(pHash);
      return this;
    }

    /**
     * Setter for mRevisionsToRestore.
     * 
     * @param pRevToRestore
     *          to be set
     * @return reference to the builder object
     */
    public Builder setRevisionsToRestore(final int pRevToRestore) {
      checkArgument(pRevToRestore > 0, "pRevisionsToRestore must be > 0!");
      mRevisionsToRestore = pRevToRestore;
      return this;
    }

    /**
     * Consistency level.
     * 
     * @param pConsistency
     *          the consistency level
     * @return reference to the builder object
     */
    public Builder setConsistency(@Nonnull final EConsistency pConsistency) {
      mConsistency = checkNotNull(pConsistency);
      return this;
    }

    /**
     * Determines if text-compression should be used or not.
     * 
     * @param pCompressionLevel
     *          to be set
     * @return reference to the builder object
     */
    public Builder useCompression(final boolean pCompression) {
      mCompression = pCompression;
      return this;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Type", mType).add("RevisionKind", mRevisionKind).add(
        "HashKind", mHashKind).toString();
    }

    /**
     * Building a new {@link ResourceConfiguration} with immutable fields.
     * 
     * @return a new {@link ResourceConfiguration} instance
     */
    public ResourceConfiguration build() {
      return new ResourceConfiguration(this);
    }
  }
}
