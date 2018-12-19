package org.unclesniper.arceye.stage;

import java.util.List;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/*   [s0]
 * forward:
 *   s0 <- [s1]
 * forward:
 *   s0 <- s1 <- [s2]
 * backward:
 *   s0 <- [s1'] -> s2
 * forward:
 *   s0 <- s1' -> s2
 *             <- [s3]
 * backward:
 *   s0 <- [s1''] -> s2
 *                -> s3
 */

public class History<StateT> {

	public static final class Snapshot<StateT> {

		public static final class NextLink<StateT> {

			private long nextID;

			private Snapshot<StateT> next;

			private NextLink(long nextID, Snapshot<StateT> next) {
				this.nextID = nextID;
				this.next = next;
			}

			public long getNextID() {
				return nextID;
			}

			public Snapshot<StateT> getNext() {
				return next;
			}

		}

		public static int STATIC_PART_BUFFER_SIZE = 16;

		private final History<StateT> history;

		private long id;

		private final long stratum;

		private StateT state;

		private long previousID;

		private Snapshot<StateT> previous;

		private List<NextLink<StateT>> nextLinks;

		private Snapshot(History<StateT> history, StateT state) {
			this.history = history;
			id = -1l;
			stratum = 0l;
			this.state = state;
			previousID = -1l;
		}

		private Snapshot(History<StateT> history, long id, long stratum, StateT state, long previousID) {
			this.history = history;
			this.id = id;
			this.stratum = stratum;
			this.state = state;
			this.previousID = previousID;
		}

		public History<StateT> getHistory() {
			return history;
		}

		public long getID() {
			return id;
		}

		public long getStratum() {
			return stratum;
		}

		public StateT getState() {
			return state;
		}

		public long getPreviousID() {
			return previousID;
		}

		public Snapshot<StateT> getPrevious() {
			return previous;
		}

		public Iterable<NextLink<StateT>> getNextLinks() {
			return nextLinks;
		}

		public int getNextLinkCount() {
			return nextLinks == null ? 0 : nextLinks.size();
		}

		private ByteBuffer getSaveBuffer() {
			ByteBuffer buffer = history.ioBuffer;
			int haveSize = buffer == null ? 0 : buffer.capacity();
			int wantSize = Snapshot.STATIC_PART_BUFFER_SIZE + getNextLinkCount() * 8 + 4
					+ history.stateIO.getNodeBufferSize();
			if(haveSize < wantSize)
				history.ioBuffer = buffer = ByteBuffer.allocate(wantSize);
			return buffer;
		}

		private void saveThisNode(Snapshot<StateT> skipForward, boolean backward, StageFile stage) {
			if(id >= 0l)
				return;
			ByteBuffer buffer = getSaveBuffer();
			synchronized(buffer) {
				buffer.clear();
				buffer.putLong(stratum).putLong(backward ? previousID : -1l);
				history.stateIO.writeNode(state, buffer);
				if(nextLinks != null) {
					buffer.putInt(nextLinks.size());
					for(NextLink<StateT> link : nextLinks) {
						if(skipForward != null && link.next == skipForward)
							buffer.putLong(-1l);
						else
							buffer.putLong(link.nextID);
					}
				}
				else
					buffer.putInt(0);
				buffer.flip();
				id = (stage != null ? stage : history.stage).writeChunk(buffer);
			}
		}

		private long saveBackward(long minCachedStratum, Snapshot<StateT> skipForward) {
			if(id < 0l) {
				if(stratum > 0l && previousID < 0l)
					previousID = previous.saveBackward(minCachedStratum, this);
				saveThisNode(skipForward, true, null);
			}
			if(stratum == minCachedStratum)
				previous = null;
			return id;
		}

		private long saveForward(long maxCachedStratum) {
			if(id < 0l) {
				if(nextLinks != null) {
					for(NextLink<StateT> link : nextLinks) {
						if(link.nextID < 0l && link.next != null) {
							link.nextID = link.next.saveForward(maxCachedStratum);
							if(stratum == maxCachedStratum)
								link.next = null;
						}
					}
				}
				saveThisNode(null, false, null);
			}
			return id;
		}

		public void saveAll() {
			if(id >= 0l)
				return;
			long stratumDelta = (long)history.maxCachedStrata;
			long minCachedStratum = stratum - stratumDelta;
			if(minCachedStratum < 0l)
				minCachedStratum = 0l;
			if(stratum > 0l && previousID < 0l)
				previousID = previous.saveBackward(minCachedStratum, this);
			if(stratum == minCachedStratum)
				previous = null;
			long maxCachedStratum = stratum + stratumDelta;
			if(maxCachedStratum < 0l)
				maxCachedStratum = Long.MAX_VALUE;
			if(nextLinks != null) {
				for(NextLink<StateT> link : nextLinks) {
					if(link.nextID < 0l && link.next != null) {
						link.nextID = link.next.saveForward(maxCachedStratum);
						if(stratum == maxCachedStratum)
							link.next = null;
					}
				}
			}
			saveThisNode(null, true, null);
		}

		public void liftAll() {
			// lift backward
			Snapshot<StateT> node = this;
			while(node.stratum > 0l) {
				if(node.previous == null)
					node.previous = history.loadSnapshot(node.previousID, -1l, node);
				node.id = -1l;
				node = node.previous;
			}
			// lift forward
			liftForward();
		}

		private void liftForward() {
			if(nextLinks != null) {
				for(NextLink<StateT> link : nextLinks) {
					if(link.next == null && link.nextID >= 0l)
						link.next = history.loadSnapshot(link.nextID, -1l, null);
					if(link.next != null)
						link.next.liftForward();
				}
			}
			id = -1l;
		}

		private void updateCacheLevel() {
			long stratumDelta = (long)history.maxCachedStrata;
			// update backward
			long minCachedStratum = stratum - stratumDelta;
			if(minCachedStratum < 0l)
				minCachedStratum = 0l;
			Snapshot<StateT> node = this, prev = null;
			for(;;) {
				if(node.stratum > minCachedStratum) {
					if(node.previous == null)
						node.previous = history.loadSnapshot(node.previousID, -1l, node);
					prev = node;
					node = node.previous;
				}
				else {
					if(node.previous != null)
						node.saveBackward(minCachedStratum, prev);
					break;
				}
			}
			// update forward
			long maxCachedStratum = stratum + stratumDelta;
			if(maxCachedStratum < 0l)
				maxCachedStratum = Long.MAX_VALUE;
			updateCacheLevelForward(maxCachedStratum);
		}

		private void updateCacheLevelForward(long maxCachedStratum) {
			if(nextLinks == null)
				return;
			if(stratum >= maxCachedStratum) {
				for(NextLink<StateT> link : nextLinks) {
					if(link.next != null) {
						link.nextID = link.next.saveForward(maxCachedStratum);
						link.next = null;
					}
				}
			}
			else {
				for(NextLink<StateT> link : nextLinks) {
					if(link.next == null) {
						if(link.nextID < 0l)
							continue;
						link.next = history.loadSnapshot(link.nextID, -1l, null);
					}
					link.next.updateCacheLevelForward(maxCachedStratum);
				}
			}
		}

		private Snapshot<StateT> mapToStage(StageFile stage) {
			return mapToStageBackward(stage, null);
		}

		private Snapshot<StateT> mapToStageBackward(StageFile stage, Snapshot<StateT> skipForward) {
			// map backward
			long newPreviousID;
			Snapshot<StateT> newPrevious;
			if(previous != null) {
				newPrevious = previous.mapToStageBackward(stage, this);
				newPreviousID = newPrevious.id;
			}
			else if(previousID >= 0l) {
				newPrevious = null;
				newPreviousID = history
						.loadSnapshot(previousID, -1l, null)
						.mapToStageBackward(stage, this)
						.id;
			}
			else {
				newPrevious = null;
				newPreviousID = -1l;
			}
			// map forward
			List<NextLink<StateT>> newNextLinks = mapNextLinksToStage(stage, skipForward);
			Snapshot<StateT> newSnapshot = new Snapshot<>(history, -1l, stratum, state, newPreviousID);
			newSnapshot.previous = newPrevious;
			newSnapshot.nextLinks = newNextLinks;
			newSnapshot.saveThisNode(null, true, stage);
			return newSnapshot;
		}

		private Snapshot<StateT> mapToStageForward(StageFile stage) {
			List<NextLink<StateT>> newNextLinks = mapNextLinksToStage(stage, null);
			Snapshot<StateT> newSnapshot = new Snapshot<>(history, -1l, stratum, state, -1l);
			newSnapshot.nextLinks = newNextLinks;
			newSnapshot.saveThisNode(null, false, stage);
			return newSnapshot;
		}

		private List<NextLink<StateT>> mapNextLinksToStage(StageFile stage, Snapshot<StateT> skipForward) {
			List<NextLink<StateT>> newNextLinks;
			if(nextLinks == null || nextLinks.isEmpty())
				newNextLinks = null;
			else {
				newNextLinks = new LinkedList<NextLink<StateT>>();
				for(NextLink<StateT> link : nextLinks) {
					long newNextID;
					Snapshot<StateT> newNext;
					if(
						skipForward != null
						&& (link.next == null || link.next == skipForward)
						&& (link.nextID < 0l || link.nextID == skipForward.id)
					) {
						newNext = null;
						newNextID = -1l;
					}
					else if(link.next != null) {
						newNext = link.next.mapToStageForward(stage);
						newNextID = newNext.id;
					}
					else if(link.nextID >= 0l) {
						newNext = null;
						newNextID = history
								.loadSnapshot(link.nextID, -1l, null)
								.mapToStageForward(stage)
								.id;
					}
					else {
						newNext = null;
						newNextID = -1l;;
					}
					newNextLinks.add(new NextLink<StateT>(newNextID, newNext));
				}
			}
			return newNextLinks;
		}

		public void reinstate() {
			long hs = history.currentState.stratum;
			if(hs == stratum)
				return;
			if(hs < stratum)
				history.redoRec(this);
			else {
				while(stratum < history.currentState.stratum)
					history.undo();
			}
		}

	}

	public static final int DEFAULT_MAX_CACHED_STRATA = 1;

	private StageFile stage;

	private NodeIO<StateT> stateIO;

	private ByteBuffer ioBuffer;

	private int maxCachedStrata = History.DEFAULT_MAX_CACHED_STRATA;

	private Snapshot<StateT> currentState;

	private int forwardTail;

	private int backwardTail;

	public History(StateT initialState, StageFile stage, NodeIO<StateT> stateIO) {
		this.stage = stage;
		this.stateIO = stateIO;
		currentState = new Snapshot<StateT>(this, initialState);
	}

	public History(StateT initialState) {
		this(initialState, null, null);
	}

	public History(StageFile stage, NodeIO<StateT> stateIO, long rootID, int maxCachedStrata, boolean attach) {
		this.stage = stage;
		this.stateIO = stateIO;
		this.maxCachedStrata = maxCachedStrata < 0 ? History.DEFAULT_MAX_CACHED_STRATA : maxCachedStrata;
		currentState = loadSnapshot(rootID, -1l, null);
		if(attach)
			updateCacheLevel();
		else
			setStage(null);
	}

	public History(StageFile stage, NodeIO<StateT> stateIO, long rootID, int maxCachedStrata) {
		this(stage, stateIO, rootID, maxCachedStrata, true);
	}

	public History(StageFile stage, NodeIO<StateT> stateIO, long rootID, boolean attach) {
		this(stage, stateIO, rootID, -1, attach);
	}

	public History(StageFile stage, NodeIO<StateT> stateIO, long rootID) {
		this(stage, stateIO, rootID, -1, true);
	}

	public final StageFile getStage() {
		return stage;
	}

	public final void setStage(StageFile stage) {
		if(stage == this.stage)
			return;
		if(stateIO != null) {
			if(this.stage == null) {
				this.stage = stage;
				saveAll();
				return;
			}
			if(stage == null)
				liftAll();
			else
				currentState = currentState.mapToStage(stage);
		}
		this.stage = stage;
	}

	private void saveAll() {
		currentState.saveAll();
		if(forwardTail > maxCachedStrata)
			forwardTail = maxCachedStrata;
		if(backwardTail > maxCachedStrata)
			backwardTail = maxCachedStrata;
	}

	private void liftAll() {
		currentState.liftAll();
		forwardTail = backwardTail = -1;
	}

	public final NodeIO<StateT> getStateIO() {
		return stateIO;
	}

	public final void setStateIO(NodeIO<StateT> stateIO) {
		if(stateIO == this.stateIO)
			return;
		if(stage != null) {
			if(this.stateIO == null) {
				this.stateIO = stateIO;
				saveAll();
				return;
			}
			else if(stateIO == null)
				liftAll();
		}
		this.stateIO = stateIO;
	}

	public final int getMaxCachedStrata() {
		return maxCachedStrata;
	}

	public final void setMaxCachedStrata(int maxCachedStrata) {
		if(maxCachedStrata < 0)
			maxCachedStrata = History.DEFAULT_MAX_CACHED_STRATA;
		if(maxCachedStrata == this.maxCachedStrata)
			return;
		this.maxCachedStrata = maxCachedStrata;
		if(stateIO != null && stage != null)
			updateCacheLevel();
	}

	private void updateCacheLevel() {
		currentState.updateCacheLevel();
		forwardTail = backwardTail = maxCachedStrata;
	}

	public final Snapshot<StateT> getCurrentState() {
		return currentState;
	}

	public final long save() {
		saveAll();
		return currentState.id;
	}

	private ByteBuffer getLoadBuffer() {
		ByteBuffer buffer = ioBuffer;
		int haveSize = buffer == null ? 0 : buffer.capacity();
		int wantSize = Snapshot.STATIC_PART_BUFFER_SIZE + 4 + stateIO.getNodeBufferSize();
		if(haveSize < wantSize)
			ioBuffer = buffer = ByteBuffer.allocate(wantSize);
		return buffer;
	}

	private Snapshot<StateT> loadSnapshot(long id, long elidedForwardID, Snapshot<StateT> elidedForward) {
		ByteBuffer buffer = getLoadBuffer();
		synchronized(buffer) {
			int nodeSize = stateIO.getNodeBufferSize();
			int readsize = Snapshot.STATIC_PART_BUFFER_SIZE + 4 + nodeSize;
			buffer.clear();
			buffer.limit(readsize);
			stage.readChunk(buffer, id);
			buffer.flip();
			long stratum = buffer.getLong();
			long previousID = buffer.getLong();
			StateT state = stateIO.readNode(buffer);
			Snapshot<StateT> snapshot = new Snapshot<>(this, id, stratum, state, previousID);
			int linkCount = buffer.getInt();
			if(linkCount > 0)
				snapshot.nextLinks = new LinkedList<Snapshot.NextLink<StateT>>();
			int bufSize = buffer.capacity();
			int batchSize = bufSize / nodeSize;
			long offset = id + Snapshot.STATIC_PART_BUFFER_SIZE + 4;
			while(linkCount > 0) {
				int chunkSize = batchSize;
				if(chunkSize > linkCount)
					chunkSize = linkCount;
				buffer.clear();
				buffer.limit(chunkSize * nodeSize);
				stage.readChunk(buffer, offset);
				offset += chunkSize * nodeSize;
				buffer.flip();
				for(int i = 0; i < chunkSize; ++i) {
					long nextID = buffer.getLong();
					if(nextID < 0l)
						snapshot.nextLinks.add(new Snapshot.NextLink<StateT>(elidedForwardID, elidedForward));
					else
						snapshot.nextLinks.add(new Snapshot.NextLink<StateT>(nextID, null));
				}
				linkCount -= chunkSize;
			}
			return snapshot;
		}
	}

	public final void advance(StateT newState) {
		if(currentState.nextLinks == null)
			currentState.nextLinks = new LinkedList<Snapshot.NextLink<StateT>>();
		else if(stage != null && stateIO != null) {
			for(Snapshot.NextLink<StateT> link : currentState.nextLinks) {
				if(link.nextID < 0l && link.next != null)
					link.nextID = link.next.saveForward(currentState.stratum);
				link.next = null;
			}
		}
		Snapshot<StateT> newSnapshot = new Snapshot<>(this, -1l,
				currentState.stratum + 1l, newState, currentState.id);
		newSnapshot.previous = currentState;
		currentState.nextLinks.add(new Snapshot.NextLink<StateT>(-1l, null));
		++backwardTail;
		currentState = newSnapshot;
		if(forwardTail == 0) {
			if(stage != null && stateIO != null)
				updateCacheLevel();
		}
		else if(forwardTail > 0)
			--forwardTail;
	}

	public final void undo() {
		if(currentState.stratum == 0l)
			throw new IllegalStateException("Nothing to undo");
		if(currentState.previous == null)
			currentState.previous = loadSnapshot(currentState.previousID, currentState.id, currentState);
		++forwardTail;
		currentState = currentState.previous;
		if(backwardTail == 0) {
			if(stage != null && stateIO != null)
				updateCacheLevel();
		}
		else if(backwardTail > 0)
			--backwardTail;
	}

	public final void undo(long desiredStratum) {
		if(desiredStratum < 0l)
			throw new IllegalArgumentException("There is no such thing as a negative stratum: " + desiredStratum);
		if(desiredStratum > currentState.stratum)
			throw new IllegalArgumentException("Redo requested as undo: Desired stratum is " + desiredStratum
					+ ", which is greater than the current stratum " + currentState.stratum);
		while(desiredStratum < currentState.stratum)
			undo();
	}

	public final void undo(Snapshot<StateT> desiredSnapshot) {
		if(desiredSnapshot.history != this)
			throw new IllegalArgumentException("Illegal undo: Cannot reinstate snapshot from another history");
		if(desiredSnapshot.stratum > currentState.stratum)
			throw new IllegalArgumentException("Redo requested as undo: Desired stratum is "
					+ desiredSnapshot.stratum + ", which is greater than the current stratum "
					+ currentState.stratum);
		while(desiredSnapshot.stratum < currentState.stratum)
			undo();
	}

	public final void redo(long nextID) {
		if(nextID < 0l)
			throw new IllegalArgumentException("There is no such thing as a negative chunk ID: " + nextID);
		if(currentState.nextLinks != null) {
			for(Snapshot.NextLink<StateT> link : currentState.nextLinks) {
				if(link.nextID == nextID) {
					redo(link);
					return;
				}
			}
		}
		throw new IllegalArgumentException("Illegal redo by chunk ID: Snapshot " + nextID
				+ " is not a direct successor of current snapshot"
				+ (currentState.id < 0l ? "" : " " + currentState.id));
	}

	public final void redo(Snapshot<StateT> desiredSnapshot) {
		if(desiredSnapshot.history != this)
			throw new IllegalArgumentException("Illegal redo: Cannot reinstate snapshot from another history");
		if(desiredSnapshot.stratum < currentState.stratum)
			throw new IllegalArgumentException("Undo requested as redo: Desired stratum is "
					+ desiredSnapshot.stratum + ", which is less than the current stratum "
					+ currentState.stratum);
		redoRec(desiredSnapshot);
	}

	private void redoRec(Snapshot<StateT> desiredSnapshot) {
		if(desiredSnapshot.stratum == currentState.stratum)
			return;
		if(desiredSnapshot.stratum > currentState.stratum + 1l)
			redoRec(desiredSnapshot.previous);
		if(currentState.nextLinks != null) {
			for(Snapshot.NextLink<StateT> link : currentState.nextLinks) {
				if(
					link.next == desiredSnapshot
					|| (
						link.next == null
						&& link.nextID >= 0l
						&& desiredSnapshot.id >= 0l
						&& link.nextID == desiredSnapshot.id
					)
				) {
					redo(link);
					break;
				}
			}
		}
		throw new IllegalStateException("Inconsistency in history during redo: Desired snapshot "
				+ (desiredSnapshot.id < 0l ? "" : desiredSnapshot.id + " ")
				+ "should be a direct successor of current snapshot"
				+ (currentState.id < 0l ? "" : " " + currentState.id) + ", but isn't");
	}

	private void redo(Snapshot.NextLink<StateT> desiredLink) {
		Snapshot<StateT> newSnapshot = desiredLink.next;
		if(newSnapshot == null) {
			newSnapshot = loadSnapshot(desiredLink.nextID, -1l, null);
			newSnapshot.previous = currentState;
			newSnapshot.previousID = currentState.id;
		}
		for(Snapshot.NextLink<StateT> link : currentState.nextLinks) {
			if(link == desiredLink)
				link.nextID = -1l;
			else if(link.nextID < 0l && link.next != null)
				link.nextID = link.next.saveForward(currentState.stratum);
			link.next = null;
		}
		++backwardTail;
		currentState = newSnapshot;
		if(forwardTail == 0) {
			if(stage != null && stateIO != null)
				updateCacheLevel();
		}
		else if(forwardTail > 0)
			--forwardTail;
	}

}
