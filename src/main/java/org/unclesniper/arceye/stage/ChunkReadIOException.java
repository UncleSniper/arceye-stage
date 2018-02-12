package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.IOException;

public class ChunkReadIOException extends StagingException {

	private final File stageFile;

	private final IOException ioException;

	public ChunkReadIOException(File stageFile, IOException ioException) {
		super("I/O error reading chunk from stage file " + stageFile.getAbsolutePath()
				+ (ioException == null || ioException.getMessage() == null ? "" : ": " + ioException.getMessage()),
				ioException);
		this.stageFile = stageFile;
		this.ioException = ioException;
	}

	public File getStageFile() {
		return stageFile;
	}

	public IOException getIOException() {
		return ioException;
	}

}
