package org.unclesniper.arceye.stage;

import java.nio.ByteBuffer;

public interface NodeIO<NodeT> {

	int getMinNodeBufferSize();

	long writeNode(NodeT node, ByteBuffer buffer, StageFile file);

	NodeT readNode(ByteBuffer buffer, StageFile file);

}
