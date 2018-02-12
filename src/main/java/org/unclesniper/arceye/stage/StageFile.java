package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class StageFile {

	private final File path;

	private final FileChannel channel;

	public StageFile(File path) throws IOException {
		this.path = path;
		channel = FileChannel.open(path.toPath(),
				StandardOpenOption.READ, StandardOpenOption.WRITE,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
				StandardOpenOption.DSYNC);
	}

	public File getPath() {
		return path;
	}

	public FileChannel getChannel() {
		return channel;
	}

}
