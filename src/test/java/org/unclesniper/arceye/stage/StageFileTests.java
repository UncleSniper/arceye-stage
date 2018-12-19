package org.unclesniper.arceye.stage;

import java.io.File;
import org.junit.Test;
import java.util.Random;
import java.util.Arrays;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.ClosedChannelException;
import static org.assertj.core.api.Assertions.assertThat;

public class StageFileTests {

	public static StageFile makeStage(boolean truncate) throws IOException {
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		return new StageFile(file, truncate);
	}

	private static boolean isClosed(FileChannel channel) throws IOException {
		try {
			channel.position();
			return false;
		}
		catch(ClosedChannelException cce) {
			return true;
		}
	}

	@Test
	public void truncate() throws IOException {
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		try(FileOutputStream stream = new FileOutputStream(file)) {
			stream.write(new byte[] {1, 2, 3});
		}
		assertThat(file.length()).isEqualTo(3l);
		new StageFile(file, false).close();
		assertThat(file.length()).isEqualTo(3l);
		new StageFile(file, true).close();
		assertThat(file.length()).isZero();
	}

	@Test
	public void pathOpened() throws IOException {
		Random random = new Random();
		byte[] junk = new byte[random.nextInt(40) + 20];
		random.nextBytes(junk);
		ByteBuffer buffer = ByteBuffer.allocate(junk.length);
		buffer.put(junk);
		buffer.rewind();
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		try(StageFile stage = new StageFile(file, true)) {
			FileChannel channel = stage.getChannel();
			while(buffer.remaining() > 0)
				channel.write(buffer);
		}
		assertThat(file.length()).isEqualTo(junk.length);
		byte[] readBack = new byte[junk.length];
		try(FileInputStream stream = new FileInputStream(file)) {
			int offset = 0;
			while(offset < readBack.length) {
				int count = stream.read(readBack, offset, readBack.length - offset);
				assertThat(count).isGreaterThan(0);
				offset += count;
			}
		}
		assertThat(readBack).isEqualTo(junk);
	}

	@Test
	public void pathReturned() throws IOException {
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		try(StageFile stage = new StageFile(file, true)) {
			assertThat(stage.getPath()).isEqualTo(file);
		}
	}

	@Test
	public void close() throws IOException {
		try(StageFile stage = StageFileTests.makeStage(true)) {
			FileChannel channel = stage.getChannel();
			assertThat(StageFileTests.isClosed(channel)).isFalse();
			stage.close();
			assertThat(StageFileTests.isClosed(channel)).isTrue();
		}
	}

	@Test
	public void readFully() throws IOException {
		Random random = new Random();
		byte[] junk = new byte[30];
		random.nextBytes(junk);
		ByteBuffer buffer = ByteBuffer.allocate(junk.length);
		buffer.put(junk);
		buffer.position(10);
		buffer.limit(20);
		File file = File.createTempFile("stage", null);
		file.deleteOnExit();
		try(FileOutputStream stream = new FileOutputStream(file)) {
			stream.write(junk);
		}
		try(StageFile stage = new StageFile(file, false)) {
			stage.readChunk(buffer, 5l);
			assertThat(stage.getChannel().position()).isEqualTo(0l);
		}
		assertThat(buffer.position()).isEqualTo(20);
		assertThat(buffer.limit()).isEqualTo(20);
		buffer.clear();
		byte[] head = new byte[10];
		buffer.get(head);
		byte[] center = new byte[10];
		buffer.get(center);
		byte[] tail = new byte[10];
		buffer.get(tail);
		assertThat(head).isEqualTo(Arrays.copyOfRange(junk, 0, 10));
		assertThat(center).isEqualTo(Arrays.copyOfRange(junk, 5, 15));
		assertThat(tail).isEqualTo(Arrays.copyOfRange(junk, 20, 30));
	}

	@Test(expected = ChunkOffsetOutOfRangeException.class)
	public void readBeyond() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(10);
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.readChunk(buffer, 0l);
		}
	}

	@Test(expected = ChunkOffsetOutOfRangeException.class)
	public void readAfter() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(10);
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.readChunk(buffer, 20l);
		}
	}

	@Test
	public void readAfterEmpty() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(1);
		buffer.limit(0);
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.readChunk(buffer, 10l);
		}
		assertThat(buffer.limit()).isEqualTo(0);
	}

	@Test(expected = NullPointerException.class)
	public void readNullBuffer() throws IOException {
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.readChunk(null, 0l);
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void readNegative() throws IOException {
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.readChunk(ByteBuffer.allocate(10), -5l);
		}
	}

	@Test
	public void readInterruptedState() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(1);
		try(StageFile stage = StageFileTests.makeStage(true)) {
			stage.writeChunk(buffer);
			Thread.currentThread().interrupt();
			buffer.rewind();
			stage.readChunk(buffer, 0l);
			assertThat(Thread.interrupted()).isTrue();
			buffer.rewind();
			stage.readChunk(buffer, 0l);
			assertThat(Thread.interrupted()).isFalse();
		}
	}

	@Test
	public void readReopen() throws IOException {
		Random random = new Random();
		byte[] junk = new byte[30];
		random.nextBytes(junk);
		ByteBuffer buffer = ByteBuffer.allocate(junk.length);
		buffer.put(junk);
		try(StageFile stage = StageFileTests.makeStage(true)) {
			buffer.rewind();
			stage.writeChunk(buffer);
			stage.getChannel().close();
			buffer.rewind();
			buffer.limit(15);
			stage.readChunk(buffer, 7l);
		}
		byte[] readBack = new byte[15];
		buffer.rewind();
		buffer.get(readBack);
		assertThat(readBack).isEqualTo(Arrays.copyOfRange(junk, 7, 7 + 15));
		assertThat(buffer.limit()).isEqualTo(15);
	}

}
