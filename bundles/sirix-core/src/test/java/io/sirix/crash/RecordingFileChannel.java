package io.sirix.crash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Test-only {@link FileChannel} that APPLIES every operation to a real delegate channel (so the
 * writer's own reads, {@code size()}-based append offsets, etc. all behave exactly like
 * production) while RECORDING every content-mutating operation and every {@code force()} barrier
 * into a {@link PowerLossRecorder}.
 *
 * <p>The recorded (sequence, offset, bytes) stream plus the force barriers are the input to
 * {@link CrashStateMaterializer}, which reconstructs candidate post-power-loss file images:
 * everything up to the last completed {@code force()} on a file is durable; writes after it may
 * be lost, applied, or torn in any combination.
 *
 * <p>Ordering note: the sequence number is assigned <em>after</em> the delegate call returns,
 * under the recorder's lock. With SirixDB's single commit thread (synchronous {@code commit()})
 * the recorded order is the issue order; concurrent writers would need issue-time sequencing.
 */
final class RecordingFileChannel extends FileChannel {

  private final FileChannel delegate;
  private final PowerLossRecorder recorder;
  private final PowerLossRecorder.TargetFile target;
  private final PowerLossRecorder.WriteDurability writeDurability;

  RecordingFileChannel(final FileChannel delegate, final PowerLossRecorder recorder,
      final PowerLossRecorder.TargetFile target) {
    this(delegate, recorder, target, PowerLossRecorder.WriteDurability.NONE);
  }

  RecordingFileChannel(final FileChannel delegate, final PowerLossRecorder recorder,
      final PowerLossRecorder.TargetFile target, final PowerLossRecorder.WriteDurability writeDurability) {
    this.delegate = delegate;
    this.recorder = recorder;
    this.target = target;
    this.writeDurability = writeDurability;
  }

  private void recordWritten(final ByteBuffer preWriteView, final long position, final int written) {
    if (written <= 0) {
      return;
    }
    final byte[] data = new byte[written];
    preWriteView.get(data);
    recorder.recordWrite(target, position, data, writeDurability);
  }

  // ---- content-mutating operations: apply + record ----

  @Override
  public int write(final ByteBuffer src) throws IOException {
    final long position = delegate.position();
    final ByteBuffer view = src.duplicate();
    final int written = delegate.write(src);
    recordWritten(view, position, written);
    return written;
  }

  @Override
  public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
    long total = 0;
    for (int i = offset; i < offset + length; i++) {
      total += write(srcs[i]);
    }
    return total;
  }

  @Override
  public int write(final ByteBuffer src, final long position) throws IOException {
    final ByteBuffer view = src.duplicate();
    final int written = delegate.write(src, position);
    recordWritten(view, position, written);
    return written;
  }

  @Override
  public FileChannel truncate(final long size) throws IOException {
    delegate.truncate(size);
    recorder.recordTruncate(target, size);
    return this;
  }

  @Override
  public void force(final boolean metaData) throws IOException {
    delegate.force(metaData);
    recorder.recordForce(target, metaData);
  }

  // ---- pass-through operations ----

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    return delegate.read(dst);
  }

  @Override
  public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
    return delegate.read(dsts, offset, length);
  }

  @Override
  public int read(final ByteBuffer dst, final long position) throws IOException {
    return delegate.read(dst, position);
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
  public long transferTo(final long position, final long count, final WritableByteChannel channelTarget)
      throws IOException {
    return delegate.transferTo(position, count, channelTarget);
  }

  @Override
  public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {
    // Not used by the sirix writer; if it ever is, the harness must record it instead of bypass.
    throw new UnsupportedOperationException("transferFrom is not recorded — extend RecordingFileChannel first");
  }

  @Override
  public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {
    if (mode != MapMode.READ_ONLY) {
      // A writable mapping would mutate the file invisibly to the recorder.
      throw new UnsupportedOperationException("writable map() is not recorded — extend RecordingFileChannel first");
    }
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
  }
}
