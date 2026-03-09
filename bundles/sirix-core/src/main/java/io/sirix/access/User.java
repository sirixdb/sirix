package io.sirix.access;

import static java.util.Objects.hash;
import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.UUID;
import io.sirix.utils.ToStringHelper;

public final class User {
  private final String userName;

  private final UUID userId;

  public User(final String userName, final UUID userId) {
    this.userName = requireNonNull(userName);
    this.userId = requireNonNull(userId);
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
    return ToStringHelper.of(this).add("userId", userId).add("userName", userName).toString();
  }
}
