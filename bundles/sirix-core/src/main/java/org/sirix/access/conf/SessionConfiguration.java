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

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.sirix.api.Database;
import org.sirix.api.Session;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>SessionConfiguration</h1>
 *
 * <p>
 * Holds the {@link Session}-wide settings that can not change within the
 * runtime of a {@link Session}. This includes stuff like commit-threshold and
 * number of usable write/read transactions. Each {@link SessionConfiguration}
 * is only bound through the location to a {@link Database} and related
 * resources.
 * </p>
 */
public final class SessionConfiguration {

	// STATIC STANDARD FIELDS
	/** Number of concurrent exclusive write transactions. */
	public static final int MAX_WRITE_TRANSACTIONS = 1;

	/** Number of concurrent read transactions. */
	public static final int MAX_READ_TRANSACTIONS = 512;

	/** Commit threshold. */
	public static final int COMMIT_THRESHOLD = 262144;

	/** Default User. */
	public static final String DEFAULT_USER = "ALL";

	/**
	 * Determines if logs should be dumped to persistent storage at first during a
	 * commit or not.
	 */
	public static final boolean DUMP_LOGS = false;
	// END STATIC STANDARD FIELDS

	// MEMBERS FOR FLEXIBLE FIELDS
	/** Number of node modifications until an automatic commit occurs. */
	public final int mCommitThreshold;

	/** User for this session. */
	public final String mUser;
	// END MEMBERS FOR FIXED FIELDS

	/** ResourceConfiguration for this ResourceConfig. */
	private final String mResource;

	/**
	 * Determines if logs should be dumped to persistent storage at first during a
	 * commit or not.
	 */
	private final boolean mDumpLogs;

	/**
	 * Convenience constructor using the standard settings.
	 *
	 * @param builder
	 *          {@link Builder} reference
	 */
	private SessionConfiguration(final SessionConfiguration.Builder builder) {
		mCommitThreshold = builder.mCommitThreshold;
		mUser = builder.mUser;
		mResource = builder.mResource;
		mDumpLogs = builder.mDumpLogs;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mUser, mResource);
	}

	@Override
	public final boolean equals(final @Nullable Object obj) {
		if (obj instanceof SessionConfiguration) {
			final SessionConfiguration other = (SessionConfiguration) obj;
			return Objects.equal(mUser, other.mUser)
					&& Objects.equal(mResource, other.mResource);
		}
		return false;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("Resource", mResource)
				.add("Commit threshold", mCommitThreshold).add("User", mUser)
				.toString();
	}

	/**
	 * Get the resource.
	 *
	 * @return the resource file name
	 */
	public String getResource() {
		return mResource;
	}

	/**
	 * Dump the logs to persistent store or not.
	 *
	 * @return {@code true} if it should be dumped, {@code false} otherwise
	 */
	public boolean dumpLogs() {
		return mDumpLogs;
	}

	/**
	 * Get a new builder instance.
	 *
	 * @param resource
	 *          the name of the resource
	 * @throws NullPointerException
	 *           if {@code resource} is {@code null}
	 * @return {@link Builder} instance
	 */
	public static Builder newBuilder(final String resource) {
		return new Builder(resource);
	}

	/**
	 * Builder class for generating new {@link SessionConfiguration} instance.
	 */
	public static final class Builder {

		/** Number of node modifications until an automatic commit occurs. */
		private int mCommitThreshold = SessionConfiguration.COMMIT_THRESHOLD;

		/** User for this session. */
		private String mUser = SessionConfiguration.DEFAULT_USER;

		/**
		 * Determines if logs should be dumped to persistent storage at first during
		 * a commit or not.
		 */
		private boolean mDumpLogs = SessionConfiguration.DUMP_LOGS;

		/** Resource for the this session. */
		private final String mResource;

		/**
		 * Constructor for the {@link Builder} with fixed fields to be set.
		 *
		 * @param resource
		 *          the resource
		 */
		public Builder(final String resource) {
			mResource = checkNotNull(resource);
		}

		/**
		 * Commit threshold.
		 *
		 * @param commitThreshold
		 *          new value for field
		 * @return reference to the builder object
		 */
		public Builder commitThreshold(final @Nonnegative int commitThreshold) {
			checkArgument(commitThreshold < 100, "Value must be > 100!");
			mCommitThreshold = commitThreshold;
			return this;
		}

		/**
		 * The user accessing the resource.
		 *
		 * @param user
		 *          the user accessing the resource
		 * @return reference to the builder object
		 */
		public Builder user(final String user) {
			mUser = checkNotNull(user);
			return this;
		}

		/**
		 * Dump transaction-logs to persistent storage at first during a commit.
		 *
		 * @return reference to the builder object
		 */
		public Builder dumpLogs() {
			mDumpLogs = true;
			return this;
		}

		/**
		 * Building a new {@link SessionConfiguration} with immutable fields.
		 *
		 * @return a new {@link SessionConfiguration}.
		 */
		public SessionConfiguration build() {
			return new SessionConfiguration(this);
		}
	}
}
