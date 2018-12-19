package org.unclesniper.arceye.stage;

import java.nio.ByteBuffer;

public class ArcStack<ElementT> {

	public static final class Node<ElementT> {

		private static int STATIC_PART_BUFFER_SIZE = 16;

		private final ArcStack<ElementT> stack;

		private long id;

		private final long height;

		private final ElementT payload;

		private final long parentID;

		private Node<ElementT> parent;

		public Node(ArcStack<ElementT> stack, long id, long height, ElementT payload,
				long parentID, Node<ElementT> parent) {
			this.stack = stack;
			this.id = id;
			this.height = height;
			this.payload = payload;
			this.parentID = parentID;
			this.parent = parent;
		}

		public Node(ArcStack<ElementT> stack, long height, ElementT payload, long parentID, Node<ElementT> parent) {
			this(stack, -1l, height, payload, parentID, parent);
		}

		public Node(ArcStack<ElementT> stack, long id, long height, ElementT payload) {
			this(stack, id, height, payload, -1l, null);
		}

		public Node(ArcStack<ElementT> stack, long height, ElementT payload) {
			this(stack, -1l, height, payload, -1l, null);
		}

		public ArcStack<ElementT> getStack() {
			return stack;
		}

		public long getID() {
			return id;
		}

		public long getHeight() {
			return height;
		}

		public ElementT getPayload() {
			return payload;
		}

		public long getParentID() {
			return parentID;
		}

		public Node<ElementT> getParent() {
			return parent;
		}

	}

	private class WholeNodeIO implements NodeIO<Node<ElementT>> {

		public WholeNodeIO() {}

		public int getNodeBufferSize() {
			return Node.STATIC_PART_BUFFER_SIZE + elementIO.getNodeBufferSize();
		}

		public long writeNode(Node<ElementT> node, ByteBuffer buffer, StageFile file) {
			buffer.clear();
			buffer.putLong(node.height).putLong(node.parentID);
			elementIO.writeNode(node.payload, buffer);
			buffer.flip();
			return stage.writeChunk(buffer);
		}

		public void writeNode(Node<ElementT> node, ByteBuffer buffer) {
			buffer.putLong(node.height).putLong(node.parentID);
			elementIO.writeNode(node.payload, buffer);
		}

		public Node<ElementT> readNode(ByteBuffer buffer, StageFile file, long offset) {
			buffer.rewind();
			buffer.limit(Node.STATIC_PART_BUFFER_SIZE + elementIO.getNodeBufferSize());
			stage.readChunk(buffer, offset);
			buffer.flip();
			long height = buffer.getLong();
			long parentID = buffer.getLong();
			ElementT payload = elementIO.readNode(buffer);
			return new Node<ElementT>(ArcStack.this, offset, height, payload, parentID, null);
		}

		public Node<ElementT> readNode(ByteBuffer buffer) {
			long height = buffer.getLong();
			long parentID = buffer.getLong();
			ElementT payload = elementIO.readNode(buffer);
			return new Node<ElementT>(ArcStack.this, -1l, height, payload, parentID, null);
		}

	}

	public static final int DEFAULT_MAX_CACHED_NODES = 8;

	private StageFile stage;

	private NodeIO<ElementT> elementIO;

	private ByteBuffer ioBuffer;

	private int maxCachedNodes = ArcStack.DEFAULT_MAX_CACHED_NODES;

	private int cachedNodes;

	private Node<ElementT> top;

	private final NodeIO<Node<ElementT>> nodeIO = new WholeNodeIO();

	public ArcStack() {}

	public ArcStack(StageFile stage, NodeIO<ElementT> elementIO) {
		this.stage = stage;
		this.elementIO = elementIO;
	}

	public ArcStack(StageFile stage, NodeIO<ElementT> elementIO, long topID, int maxCachedNodes, boolean attach) {
		this.stage = stage;
		this.elementIO = elementIO;
		this.maxCachedNodes = maxCachedNodes <= 0 ? ArcStack.DEFAULT_MAX_CACHED_NODES : maxCachedNodes;
		if(topID >= 0l) {
			ByteBuffer buffer = getLoadBuffer();
			synchronized(buffer) {
				top = nodeIO.readNode(buffer, stage, topID);
			}
			cachedNodes = 1;
		}
		if(!attach)
			setStage(null);
	}

	private ByteBuffer getLoadBuffer() {
		ByteBuffer buffer = ioBuffer;
		int haveSize = buffer == null ? 0 : buffer.capacity();
		int wantSize = Node.STATIC_PART_BUFFER_SIZE + elementIO.getNodeBufferSize();
		if(haveSize < wantSize)
			ioBuffer = buffer = ByteBuffer.allocate(wantSize);
		return buffer;
	}

	public final StageFile getStage() {
		return stage;
	}

	public final void setStage(StageFile stage) {
		if(stage == this.stage)
			return;
		if(elementIO != null) {
			if(this.stage == null) {
				this.stage = stage;
				saveAll();
				return;
			}
			if(stage == null)
				liftAll();
			else if(top != null)
				top = mapToStage(stage);
		}
		this.stage = stage;
	}

	private void saveAll() {
		ByteBuffer buffer = getLoadBuffer();
		synchronized(buffer) {
			for(Node<ElementT> node = top; node != null; node = node.parent) {
				if(node.id >= 0l)
					break;
				node.id = nodeIO.writeNode(node, buffer, stage);
			}
		}
	}

	private void liftAll() {
		//TODO
	}

	private Node<ElementT> mapToStage(StageFile stage) {
		//TODO
		return null;
	}

}
