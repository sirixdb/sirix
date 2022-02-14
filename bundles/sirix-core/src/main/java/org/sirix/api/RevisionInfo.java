package org.sirix.api;

import com.google.common.base.MoreObjects;
import org.sirix.access.User;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class RevisionInfo {
  private final User user;

  private final int revision;

  private final Instant revisionTimestamp;

  private final String commitMessage;

  private int hash;

  public RevisionInfo(final User user, final int revision, final Instant revisionTimestamp,
      final String commitMessage) {
    this.user = checkNotNull(user);

    checkArgument(revision >= 0);

    this.revision = revision;
    this.revisionTimestamp = checkNotNull(revisionTimestamp);
    this.commitMessage = commitMessage;
  }

  public User getUser() {
    return user;
  }

  public int getRevision() {
    return revision;
  }

  public Instant getRevisionTimestamp() {
    return revisionTimestamp;
  }

  public Optional<String> getCommitMessage() {
    return Optional.ofNullable(commitMessage);
  }

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(user, revision, revisionTimestamp, commitMessage);
    }
    return hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof RevisionInfo))
      return false;

    final RevisionInfo otherRevisionInfo = (RevisionInfo) other;

    return this.user == otherRevisionInfo.user && this.revision == otherRevisionInfo.revision
        && revisionTimestamp.equals(otherRevisionInfo.revisionTimestamp)
        && Objects.equals(this.commitMessage, otherRevisionInfo.commitMessage);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("user", user)
                      .add("revision", revision)
                      .add("revisionTimestamp", revisionTimestamp)
                      .add("commitMessage", commitMessage)
                      .toString();
  }
}
