package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.hash;
import java.util.Objects;
import java.util.UUID;
import com.google.common.base.MoreObjects;

public final class User {
  private final String userName;

  private final UUID userId;

  public User(final String userName, final UUID userId) {
    this.userName = checkNotNull(userName);
    this.userId = checkNotNull(userId);
  }

  public UUID getId() {
    return userId;
  }

  public String getName() {
    return userName;
  }

  @Override
  public int hashCode() {
    return hash(userName, userId);
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof User))
      return false;

    final User user = (User) other;
    return Objects.equals(userId, user.userId) && Objects.equals(userName, user.userName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("userId", userId).add("userName", userName).toString();
  }
}
