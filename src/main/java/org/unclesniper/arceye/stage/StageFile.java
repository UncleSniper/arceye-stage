package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

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
 * up adjacent in the underlying file. It must be understood
 * that the fact that writes synchronize on the backing
 * <tt>FileChannel</tt> object is <b>not</b> part of the public
 * API and may not be relied upon in order to force
 * multiple chunks to be written adjacently. In a similar
 * sentiment, while the <tt>FileChannel</tt> object can be
 * {@link #getChannel() obtained} for completeness,
 * client code <b>must not</b> attempt to modify any
 * data already written to the file and <b>must</b>
 * ensure that no concurrent attempt to write to the
 * channel can occur if data is to be appended. If
 * multiple writes must be performed without risking
 * intervening writes by other threads,
 * {@link #sequence(Runnable)} may be used instead.
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
	 * Retained for information purposes.
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
	private final FileChannel channel;

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
	 * 	pathname of stage file to be opened
	 * @param truncate
	 * 	<tt>true</tt> if and only if stage file should be
	 * 	truncated to zero length
	 * @throws IOException
	 * 	if an I/O error occurs when attemping to open the file
	 *
	 * @since 0.1
	 */
	public StageFile(File path, boolean truncate) throws IOException {
		this.path = path;
		channel = FileChannel.open(path.toPath(), truncate ? StageFile.TRUNC_OPTIONS : StageFile.NOTRUNC_OPTIONS);
	}

	public File getPath() {
		return path;
	}

	public FileChannel getChannel() {
		return channel;
	}

	public void close() throws IOException {
		if(channel.isOpen())
			channel.close();
	}

	public void readChunk(ByteBuffer buffer, long offset) {
		try {
			while(buffer.remaining() > 0) {
				int count = channel.read(buffer, offset);
				if(count < 0)
					throw new ChunkOffsetOutOfRangeException(path, offset);
				offset += count;
			}
		}
		catch(IOException ioe) {
			throw new ChunkReadIOException(path, ioe);
		}
	}

	public long writeChunk(ByteBuffer buffer) {
		try {
			synchronized(channel) {
				long start = channel.size();
				long offset = start;
				while(buffer.remaining() > 0)
					offset += channel.write(buffer, offset);
				return start;
			}
		}
		catch(IOException ioe) {
			throw new ChunkWriteIOException(path, ioe);
		}
	}

	public void sequence(Runnable task) {
		synchronized(channel) {
			task.run();
		}
	}

}
