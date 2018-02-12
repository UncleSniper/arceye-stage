package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class StageFile {

	private static final OpenOption[] NOTRUNC_OPTIONS;
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

	private final File path;

	private final FileChannel channel;

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

}
