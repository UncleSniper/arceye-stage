package org.unclesniper.arceye.stage;

public abstract class StagingException extends RuntimeException {

	public StagingException(String message) {
		super(message);
	}

	public StagingException(Throwable cause) {
		super(cause == null ? null : cause.getMessage(), cause);
	}

	public StagingException(String message, Throwable cause) {
		super(message == null && cause != null ? cause.getMessage() : message, cause);
	}

}
