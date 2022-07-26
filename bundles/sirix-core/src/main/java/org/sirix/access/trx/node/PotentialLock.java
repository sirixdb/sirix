package org.sirix.access.trx.node;

import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public final class PotentialLock implements Lock, AutoCloseable {

  private final Lock lock;

  public PotentialLock(final Lock lock) {
    this.lock = Preconditions.checkNotNull(lock);
  }

  @Override
  public void lock() {
    if (lock != null) {
      lock.lock();
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    if (lock != null) {
      lock.lockInterruptibly();
    }
  }

  @Override
  public boolean tryLock() {
    if (lock != null) {
      return lock.tryLock();
    }
    return true;
  }

  @Override
  public boolean tryLock(long time, @NotNull TimeUnit unit) throws InterruptedException {
    if (lock != null) {
      return lock.tryLock(time, unit);
    }
    return true;
  }

  @Override
  public void unlock() {
    if (lock != null) {
      lock.unlock();
    }
  }

  @NotNull
  @Override
  public Condition newCondition() {
    if (lock != null) {
      return lock.newCondition();
    }
    return null;
  }

  @Override
  public void close() throws Exception {
    if (lock != null) {
      lock.unlock();
    }
  }
}
