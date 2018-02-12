package org.unclesniper.arceye.stage;

import java.io.File;
import java.io.IOException;

public class ChunkWriteIOException extends StagingException {

	private final File stageFile;

	private final IOException ioException;

	public ChunkWriteIOException(File stageFile, IOException ioException) {
		super("I/O error writing chunk to stage file " + stageFile.getAbsolutePath()
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
