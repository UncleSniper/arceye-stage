package org.unclesniper.arceye.stage;

import java.io.File;

public class ChunkOffsetOutOfRangeException extends StagingException {

	private final File stageFile;

	private final long chunkOffset;

	public ChunkOffsetOutOfRangeException(File stageFile, long chunkOffset) {
		super("Chunk offset " + chunkOffset + " is out of range in stage file " + stageFile.getAbsolutePath());
		this.stageFile = stageFile;
		this.chunkOffset = chunkOffset;
	}

	public File getStageFile() {
		return stageFile;
	}

	public long getChunkOffset() {
		return chunkOffset;
	}

}
