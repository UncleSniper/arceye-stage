package org.unclesniper.arceye.stage;

import java.util.List;
import java.nio.ByteBuffer;

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

		public static class NextLink<StateT> {

			private long nextID;

			private Snapshot<StateT> next;

		}

		public static int STATIC_PART_BUFFER_SIZE = 16;

		private final History<StateT> history;

		private long id;

		private final long stratum;

		private final StateT state;

		private final long previousID;

		private Snapshot<StateT> previous;

		private List<NextLink<Snapshot>> nextLinks;

		public Snapshot(History<StateT> history, StateT state) {
			this.history = history;
			id = -1l;
			stratum = 0l;
			this.state = state;
			previousID = -1l;
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

		public Iterable<NextLink<Snapshot>> getNextLinks() {
			return nextLinks;
		}

		public int getNextLinkCount() {
			return nextLinks == null ? 0 : nextLinks.size();
		}

		public ByteBuffer getByteBuffer() {
			ByteBuffer buffer = history.ioBuffer;
			int haveSize = buffer == null ? 0 : buffer.capacity();
			int wantSize = Snapshot.STATIC_PART_BUFFER_SIZE + getNextLinkCount() * 8
					+ history.stateIO.getNodeBufferSize();
			if(haveSize < wantSize)
				history.ioBuffer = buffer = ByteBuffer.allocate(wantSize);
			return buffer;
		}

		public void save() {
			//TODO
		}

		public void saveAll(StageFile stage, NodeIO<StateT> stateIO) {
			//TODO
		}

		public void liftAll() {
			//TODO
		}

		public void updateCacheLevel() {
			//TODO
		}

		public Snapshot<StateT> mapToStage(StageFile stage) {
			//TODO
			return null;
		}

	}

	public static final int DEFAULT_MAX_CACHED_STRATA = 1;

	private StageFile stage;

	private NodeIO<StateT> stateIO;

	private ByteBuffer ioBuffer;

	private int maxCachedStrata = History.DEFAULT_MAX_CACHED_STRATA;

	private Snapshot<StateT> currentState;

	public History(StateT initialState, StageFile stage, NodeIO<StateT> stateIO) {
		this.stage = stage;
		this.stateIO = stateIO;
		currentState = new Snapshot<StateT>(this, initialState);
	}

	public StageFile getStage() {
		return stage;
	}

	public void setStage(StageFile stage) {
		if(stage == this.stage)
			return;
		if(stateIO != null) {
			if(this.stage == null)
				currentState.saveAll(stage, null);
			else if(stage == null)
				currentState.liftAll();
			else
				currentState = currentState.mapToStage(stage);
		}
		this.stage = stage;
	}

	public NodeIO<StateT> getStateIO() {
		return stateIO;
	}

	public void setStateIO(NodeIO<StateT> stateIO) {
		if(stateIO == this.stateIO)
			return;
		if(stage != null) {
			if(this.stateIO == null)
				currentState.saveAll(null, stateIO);
			else if(stateIO == null)
				currentState.liftAll();
		}
		this.stateIO = stateIO;
	}

	public int getMaxCachedStrata() {
		return maxCachedStrata;
	}

	public void setMaxCachedStrata(int maxCachedStrata) {
		if(maxCachedStrata < 0)
			maxCachedStrata = History.DEFAULT_MAX_CACHED_STRATA;
		if(maxCachedStrata == this.maxCachedStrata)
			return;
		this.maxCachedStrata = maxCachedStrata;
		currentState.updateCacheLevel();
	}

	public Snapshot<StateT> getCurrentState() {
		return currentState;
	}

	public void save() {
		currentState.saveAll(null, null);
	}

}
