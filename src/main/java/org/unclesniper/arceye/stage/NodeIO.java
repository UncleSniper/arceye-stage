package org.unclesniper.arceye.stage;

import java.nio.ByteBuffer;

public interface NodeIO<NodeT> {

	int getNodeBufferSize();

	long writeNode(NodeT node, ByteBuffer buffer, StageFile file);

	void writeNode(NodeT node, ByteBuffer buffer);

	NodeT readNode(ByteBuffer buffer, StageFile file, long offset);

	NodeT readNode(ByteBuffer buffer);

}
