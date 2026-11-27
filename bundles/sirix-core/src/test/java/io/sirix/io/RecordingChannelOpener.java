/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Opens real channels and records every open and force, so a test can see the fsync TXN-616 asks
 * for: no in-process observer can see one otherwise.
 */
final class RecordingChannelOpener implements ChannelOpener {

  final List<String> calls = new ArrayList<>();

  @Override
  public FileChannel open(final Path path, final OpenOption option) throws IOException {
    calls.add("open " + path + " " + option);
    return new Recording(FileChannel.open(path, option), path);
  }

  private final class Recording extends FileChannel {
    private final FileChannel delegate;
    private final Path path;

    Recording(final FileChannel delegate, final Path path) {
      this.delegate = delegate;
      this.path = path;
    }

    @Override
    public void force(final boolean metaData) throws IOException {
      delegate.force(metaData);
      calls.add("force " + path + " " + metaData);
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
      return delegate.read(dst);
    }

    @Override
    public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
      return delegate.read(dsts, offset, length);
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
      return delegate.write(src);
    }

    @Override
    public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
      return delegate.write(srcs, offset, length);
    }

    @Override
    public long position() throws IOException {
      return delegate.position();
    }

    @Override
    public FileChannel position(final long newPosition) throws IOException {
      delegate.position(newPosition);
      return this;
    }

    @Override
    public long size() throws IOException {
      return delegate.size();
    }

    @Override
    public FileChannel truncate(final long size) throws IOException {
      delegate.truncate(size);
      return this;
    }

    @Override
    public long transferTo(final long position, final long count, final WritableByteChannel target) throws IOException {
      return delegate.transferTo(position, count, target);
    }

    @Override
    public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {
      return delegate.transferFrom(src, position, count);
    }

    @Override
    public int read(final ByteBuffer dst, final long position) throws IOException {
      return delegate.read(dst, position);
    }

    @Override
    public int write(final ByteBuffer src, final long position) throws IOException {
      return delegate.write(src, position);
    }

    @Override
    public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {
      return delegate.map(mode, position, size);
    }

    @Override
    public FileLock lock(final long position, final long size, final boolean shared) throws IOException {
      return delegate.lock(position, size, shared);
    }

    @Override
    public FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
      return delegate.tryLock(position, size, shared);
    }

    @Override
    protected void implCloseChannel() throws IOException {
      delegate.close();
      calls.add("close " + path);
    }
  }
}
