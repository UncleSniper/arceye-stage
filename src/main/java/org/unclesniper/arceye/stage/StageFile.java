package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.nio.channels.ClosedChannelException;

/**
 * On-disk storage for large data structures.
 *
 * The data structure classes in this package may be stored,
 * in whole or in part, in the filesystem of the host JVM.
 * The StageFile class encapsulates a {@link FileChannel}
 * as a storage backend for such uses. An instance of this
 * class is referred to as a <i>stage</i> throughout the
 * <tt>arceye-stage</tt> documentation.
 * <p>
 * The fundamental assumption is that all data structures
 * that operate in such a manner be <i>constructive</i>
 * rather than <i>destructive</i>, meaning that any given
 * substructure is considered <b>conceptionally</b> immutable:
 * While the objects involved in the representation may
 * indeed be modified by the provided operations, any
 * substructure introspected at any time will remain
 * equivalent to the state in which it was at the moment
 * of introspection. For example, obtaining the root node
 * object of a tree-based data structure is tantamount to
 * keeping a deep copy of the entire tree, as no node will
 * ever be modified in such a way that the values or order
 * of the node payload objects will change.
 * <p>
 * Owing to this premise, data written to a stage may
 * never again be modified. The only two input/output
 * operations allowed are thus
 * {@link #readChunk(ByteBuffer, long) reading} a
 * <i>chunk</i> of bytes and
 * {@link #writeChunk(ByteBuffer) <b>appending</b>} a
 * chunk of bytes to the
 * end of the file. Each such write operation thus yields
 * a file offset, called a <i>chunk ID</i>, that uniquely
 * identifies the chunk. Reading the same number of bytes
 * from that offset will always yield the same bytes as
 * were originally written. Note that the size of a chunk
 * is <b>not</b> written to the stage and must be known
 * by the caller if the same chunk is to be read.
 * <p>
 * Since the file contents are merely a sequence of
 * bytes, and no header data (such as chunk sizes) is
 * written, the boundaries of reads need not align with
 * the boundaries of writes. If eight bytes are written
 * yielding the chunk ID <tt>2</tt>, it is guaranteed
 * that
 * <ul>
 * 	<li>
 * 		reading a chunk of four bytes from offset zero
 * 		will succeed and the last two bytes of the result
 * 		are the first two bytes of the written chunk.
 * 	</li>
 * 	<li>
 * 		reading a chunk of two bytes from offset two
 * 		will succeed and the result is the first two
 * 		bytes of the written chunk.
 * 	</li>
 * 	<li>
 * 		reading a chunk of <tt>N</tt> bytes from offset
 * 		<tt>K</tt> will succeed if (but not necessarily
 * 		<i>only if</i>) <tt>K &gt;= 0</tt> and
 * 		<tt>K + N &lt;= 10</tt>.
 * 	</li>
 * </ul>
 * <p>
 * It shall be noted that reads and writes are thread
 * safe with the granularity of these operations. If
 * two writes are performed in succession, there is no
 * guarantee (other than the absence of concurrent threads)
 * that the byte sequences of the two chunks will end
 * up adjacent in the underlying file. While the
 * <tt>FileChannel</tt> object can be
 * {@link #getChannel() obtained} for completeness,
 * client code <b>must not</b> attempt to modify any
 * data already written to the file and <b>must</b>
 * ensure that no concurrent attempt to write to the
 * channel can occur if data is to be appended. If
 * multiple writes must be performed without risking
 * intervening writes by other threads,
 * {@link #sequence(Runnable)} may be used instead.
 * <p>
 * It is also worth noting that, since the data
 * structures based on stage files are intended to
 * <i>appear</i> as though they were being kept in
 * memory, attempts are made to make the I/O operations
 * as transparent as possible. See
 * {@link #readChunk(ByteBuffer, long) readChunk} for
 * details.
 *
 * @since 0.1
 */
public final class StageFile implements Closeable {

	/**
	 * Options for non-truncating open.
	 *
	 * Used in {@link #StageFile(File, boolean)} when
	 * <tt>truncate</tt> is <tt>false</tt>. Contains
	 * <ul>
	 * 	<li>{@link StandardOpenOption#READ}</li>
	 * 	<li>{@link StandardOpenOption#WRITE}</li>
	 * 	<li>{@link StandardOpenOption#CREATE}</li>
	 * 	<li>{@link StandardOpenOption#DSYNC}</li>
	 * </ul>
	 * We include <tt>DSYNC</tt> in order to allow recovery
	 * of state in case of crashes or power failure.
	 *
	 * @since 0.1
	 */
	private static final OpenOption[] NOTRUNC_OPTIONS;

	/**
	 * Options for truncating open.
	 *
	 * Used in {@link #StageFile(File, boolean)} when
	 * <tt>truncate</tt> is <tt>true</tt>. Contains
	 * the same options as {@link #NOTRUNC_OPTIONS},
	 * plus {@link StandardOpenOption#TRUNCATE_EXISTING}.
	 *
	 * @since 0.1
	 */
	private static final OpenOption[] TRUNC_OPTIONS;

	static {
		NOTRUNC_OPTIONS = new OpenOption[] {
			StandardOpenOption.READ,
			StandardOpenOption.WRITE,
			StandardOpenOption.CREATE,
			StandardOpenOption.DSYNC
		};
		TRUNC_OPTIONS = new OpenOption[NOTRUNC_OPTIONS.length + 1];
		for(int i = 0; i < NOTRUNC_OPTIONS.length; ++i)
			TRUNC_OPTIONS[i] = NOTRUNC_OPTIONS[i];
		TRUNC_OPTIONS[NOTRUNC_OPTIONS.length] = StandardOpenOption.TRUNCATE_EXISTING;
	}

	/**
	 * Pathname of the underlying file.
	 *
	 * May be used for information purposes.
	 *
	 * @since 0.1
	 * @see #StageFile(File, boolean)
	 * @see #getPath()
	 */
	private final File path;

	/**
	 * Underlying file channel.
	 *
	 * @since 0.1
	 * @see #getChannel()
	 */
	private volatile FileChannel channel;

	/**
	 * The big stage lock.
	 *
	 * Write operations synchronize on this object.
	 * Furthermore, since
	 * {@link #reopen(boolean) reopening} the
	 * underlying file might be necessary, and
	 * doing so involves creating a new channel,
	 * any attempt to reopen the file will
	 * synchronize on this object as well, thus
	 * avoiding race conditions arising from
	 * multiple threads attempting to reopen the
	 * same file at the same time.
	 *
	 * @since 0.1
	 */
	private final Object lock = new Object();

	/**
	 * Open a stage file.
	 *
	 * All files are opened read/write, since even just
	 * traversing large structures may involve transferring
	 * control data (such as call stack frames) from memory
	 * into the stage in order to avoid resource shortages.
	 * This may change at a future time.
	 * <p>
	 * If the contents of the file are to be retained in
	 * order to recover state from a previous session,
	 * <tt>truncate</tt> may be set to <tt>false</tt>, in
	 * which case the file is left unchanged. Otherwise,
	 * the file is truncated to zero length, thus discarding
	 * all chunks previously staged.
	 * <p>
	 * Owing to the purpose of stage files, it is imperative
	 * that the same file not be opened more than once,
	 * regardless of whether the open operations are performed
	 * within the same process. For performance reasons, this
	 * is not checked or enforced at present, but this may
	 * change at a future time.
	 *
	 * @param path
	 * 	pathname of stage file to be opened;
	 * 	must not be <tt>null</tt>
	 * @param truncate
	 * 	<tt>true</tt> if and only if stage file should be
	 * 	truncated to zero length
	 * @throws NullPointerException
	 * 	if <tt>path</tt> is <tt>null</tt>
	 * @throws IOException
	 * 	if an I/O error occurs when attemping to open the file
	 *
	 * @since 0.1
	 */
	public StageFile(File path, boolean truncate) throws IOException {
		this.path = path;
		channel = FileChannel.open(path.toPath(), truncate ? StageFile.TRUNC_OPTIONS : StageFile.NOTRUNC_OPTIONS);
	}

	/**
	 * Reopen the file channel if it was closed.
	 *
	 * If the channel is open, it is retained.
	 * Otherwise, it is replaced with a newly
	 * opened channel.
	 *
	 * @param forRead
	 * 	whether to throw a {@link ChunkReadIOException}
	 * 	or a {@link ChunkWriteIOException} on I/O error
	 */
	private void reopen(boolean forRead) {
		try {
			synchronized(lock) {
				if(!channel.isOpen())
					channel = FileChannel.open(path.toPath(), StageFile.NOTRUNC_OPTIONS);
			}
		}
		catch(IOException ioe) {
			throw forRead ? new ChunkReadIOException(path, ioe) : new ChunkWriteIOException(path, ioe);
		}
	}

	/**
	 * Retrieve the pathname of the underlying file.
	 *
	 * As this same path is used to
	 * {@link #StageFile(File, boolean) open} the file,
	 * the return value is guaranteed to be non-<tt>null</tt>.
	 *
	 * @return
	 * 	pathname of the on-disk stage file
	 * @since 0.1
	 */
	public File getPath() {
		return path;
	}

	/**
	 * Retrieve the underlying file channel.
	 *
	 * The channel is exposed for completeness only.
	 * The returned object <b>must not</b> not used to
	 * <ul>
	 * 	<li>
	 * 		write to the file (unless only appending
	 * 		writes are performed within the confines of
	 * 		{@link #sequence(Runnable) sequence} to
	 * 		avoid race conditions; it is strongly
	 * 		recommended to use
	 * 		{@link #writeChunk(ByteBuffer) writeChunk}
	 * 		instead). This includes writes performed
	 * 		via memory mapping.
	 * 	</li>
	 * 	<li>
	 * 		truncate the file.
	 * 	</li>
	 * </ul>
	 * The returned object <i>may</i> be used to
	 * <ul>
	 * 	<li>
	 * 		read from the file (although using
	 * 		{@link #readChunk(ByteBuffer, long) readChunk}
	 * 		for this purpose is strongly recommended
	 * 		in any case that does not involve
	 * 		{@link FileChannel#map memory mapping} nor
	 * 		{@link FileChannel#transferTo direct transfer}).
	 * 	</li>
	 * 	<li>
	 * 		retrieve and/or modify the
	 * 		{@link FileChannel#position() position}
	 * 		of the channel, as the position is not
	 * 		used by this class.
	 * 	</li>
	 * 	<li>
	 * 		retrieve the current size of the file,
	 * 		but beware of race conditions!
	 * 	</li>
	 * 	<li>
	 * 		close the file, which is harmless to the
	 * 		operation of {@link #close()}.
	 * 	</li>
	 * 	<li>
	 * 		lock the file.
	 * 	</li>
	 * 	<li>
	 * 		flush the file. Note that the file will
	 * 		be open with
	 * 		{@link StandardOpenOption#DSYNC data synchronicity},
	 * 		so explicit flushes will be unnecessary in
	 * 		many cases. If this is
	 * 		{@link StandardOpenOption#SYNC not sufficient},
	 * 		the caller may nonetheless flush the file
	 * 		{@link FileChannel#force(boolean) explicitly}.
	 * 	</li>
	 * </ul>
	 * Note that the identity of the channel being used
	 * is subject to change without notice due to the
	 * reopen semantics. See
	 * {@link #readChunk(ByteBuffer, long) readChunk}
	 * for details. If you intend to use the channel
	 * in such a way that replacing the channel object
	 * must be avoided, use
	 * {@link #sequence(Runnable) sequence}.
	 *
	 * @return
	 * 	the channel used to read/write the underlying
	 * 	on-disk stage file
	 * @since 0.1
	 */
	public FileChannel getChannel() {
		return channel;
	}

	/**
	 * Close the underlying file channel.
	 *
	 * The channel will only be closed if it
	 * is open. Therefore, this method may
	 * be called more than once (possibly
	 * interspersed with closing
	 * {@link #getChannel() the file channel itself})
	 * with no ill effect.
	 * <p>
	 * Note that any
	 * attempt to read or write the channel
	 * via the operations provided by this
	 * class will cause the channel to be
	 * reopened. See
	 * {@link #readChunk(ByteBuffer, long) readChunk}
	 * for details.
	 *
	 * @throws IOException
	 * 	if an I/O error occurs when attempting
	 * 	to close the file
	 * @since 0.1
	 */
	public void close() throws IOException {
		synchronized(lock) {
			if(channel.isOpen())
				channel.close();
		}
	}

	/**
	 * Read a chunk of data from the stage file.
	 *
	 * The <tt>buffer</tt> indicates the destination
	 * for the bytes to be read: The <i>remaining</i>
	 * size (id est, the difference between its
	 * <i>limit</i> and its <i>position</i>) is read
	 * from the file and stored in the <tt>buffer</tt>
	 * starting at its <i>position</i>. Exactly the
	 * so indicated number of bytes are read, starting
	 * at the given <tt>offset</tt>, from the underlying
	 * {@link #getChannel() file channel}. Thus, this
	 * interface differs from the underlying
	 * {@link FileChannel#read(ByteBuffer, long)}
	 * method in, and only in, that short reads are
	 * <ul>
	 * 	<li>
	 * 		avoided, by performing as many reads as
	 * 		necessary, until the desired number of
	 * 		bytes have been retrieved.
	 * 	</li>
	 * 	<li>
	 * 		not allowed, in that
	 * 		<tt>buffer.remaining()</tt> bytes
	 * 		<b>must</b> be present in the file, starting
	 * 		at <tt>offset</tt>, as per the current size
	 * 		of the file (regardless of the number of
	 * 		bytes immediately available for a single
	 * 		read from the underlying channel).
	 * 	</li>
	 * 	<li>
	 * 		considered errors, in that a
	 * 		<tt>ChunkOffsetOutOfRangeException</tt> is
	 * 		thrown if the preceding precondition does
	 * 		not hold.
	 * 	</li>
	 * </ul>
	 * Consequently, if this method returns normally,
	 * the exact number of bytes requested are guaranteed
	 * to have been transferred. If any exception is
	 * thrown, any number of bytes, up to and including
	 * the requested number, may have been transferred;
	 * the number actually transferred can be introspected
	 * by the caller by considering by how much the
	 * <i>position</i> of the <tt>buffer</tt> has moved.
	 * Under no circumstances (regardless of whether the
	 * call returns normally or throws an exception) will
	 * the <i>limit</i> of the <tt>buffer</tt> be altered.
	 * <p>
	 * This method neither considers nor modifies the
	 * current <i>position</i> of the underlying channel.
	 * It is therefore safe to introspect and/or alter
	 * that <i>position</i> without the knowledge of
	 * <tt>this</tt> object.
	 * <p>
	 * A <tt>ChunkOffsetOutOfRangeException</tt> is
	 * thrown if and only if the end of the region to
	 * be read exceeds the end of the underlying file
	 * and that region is not empty.
	 * This includes both the case in which only
	 * part of that region is outside the file contents
	 * ({@code offset < sz && offset + buffer.remaining() > sz},
	 * where <tt>sz</tt> is the size of the file) and
	 * the case in which all of the region is outside
	 * the file contents
	 * ({@code offset >= sz && buffer.remaining() > 0}).
	 * It is not an error in this sense for zero bytes
	 * to be read, even if {@code offset >= sz}.
	 * <p>
	 * Since the intent of this class is for data
	 * structures to transparently transfer part of
	 * their structure from memory to disk and vice
	 * versa, it is not practicable to have any and
	 * all operation on such data structures declare
	 * {@link IOException} to be thrown. In order to
	 * circumvent this necessity, this method catches
	 * any <tt>IOException</tt> thrown by the underlying
	 * channel read and throws a
	 * <tt>ChunkReadIOException</tt> encapsulating the
	 * original exception instead. Since the latter
	 * class is a {@link RuntimeException}, the need
	 * for {@code throws} clauses is eliminated.
	 * <p>
	 * As a special case of <tt>IOException</tt> that
	 * the underlying channel read may evoke,
	 * {@link ClosedChannelException} is handled
	 * gracefully: As data structures using the
	 * stage should appear not to be using the stage
	 * at all, any attempt to read or
	 * {@link #writeChunk(ByteBuffer) write} a
	 * closed channel will attempt to reopen the
	 * channel and then, if successful, restart
	 * the read/write operation. If other threads
	 * repeatedly close the channel, the calling
	 * thread may thus get stuck in an endless cycle
	 * of attempted reads and subsequent reopens.
	 * Should any such reopen fail, this will be
	 * treated as a proper I/O error and a
	 * <tt>ChunkReadIOException</tt> will be thrown
	 * accordingly.
	 * <p>
	 * This method is thread safe in the face of
	 * concurrent calls to itself,
	 * {@link #writeChunk(ByteBuffer) writeChunk},
	 * {@link #sequence(Runnable) sequence},
	 * alteration of the <i>position</i> of the
	 * underlying channel, and any and all non-modifying
	 * operations on that channel, as well as closing
	 * the channel, whether via {@link #close() close}
	 * or by closing the underlying channel itself.
	 * It is, however, <b>not</b> thread safe in
	 * the face of any concurrent modifying operations
	 * on the channel, such as writes not guarded
	 * by <tt>writeChunk</tt> nor <tt>sequence</tt>.
	 *
	 * @param buffer
	 * 	destination buffer into which read bytes
	 * 	are placed
	 * @param offset
	 * 	<i>chunk ID</i> (id est, file offset) from
	 * 	which to read
	 * @throws IllegalArgumentException
	 * 	if the position is negative
	 * @throws ChunkOffsetOutOfRangeException
	 * 	if insufficient bytes are present at the
	 * 	given <tt>offset</tt>, including offsets
	 * 	beyond end-of-file
	 * @throws ChunkReadIOException
	 * 	if the underlying channel read fails with
	 * 	an {@link IOException}
	 */
	public void readChunk(ByteBuffer buffer, long offset) {
		boolean interrupted;
		for(;;) {
			interrupted = Thread.interrupted();
			try {
				while(buffer.remaining() > 0) {
					int count = channel.read(buffer, offset);
					if(count < 0)
						throw new ChunkOffsetOutOfRangeException(path, offset);
					offset += count;
				}
			}
			catch(ClosedChannelException cce) {
				reopen(true);
			}
			catch(IOException ioe) {
				throw new ChunkReadIOException(path, ioe);
			}
			finally {
				if(interrupted)
					Thread.currentThread().interrupt();
			}
		}
	}

	public long writeChunk(ByteBuffer buffer) {
		boolean interrupted;
		for(;;) {
			interrupted = Thread.interrupted();
			try {
				synchronized(lock) {
					long start = channel.size();
					long offset = start;
					while(buffer.remaining() > 0)
						offset += channel.write(buffer, offset);
					return start;
				}
			}
			catch(ClosedChannelException cce) {
				reopen(false);
			}
			catch(IOException ioe) {
				throw new ChunkWriteIOException(path, ioe);
			}
			finally {
				if(interrupted)
					Thread.currentThread().interrupt();
			}
		}
	}

	public void sequence(Runnable task) {
		synchronized(lock) {
			task.run();
		}
	}

}
