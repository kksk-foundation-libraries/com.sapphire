package com.sapphire.execution;

public class LongMessage extends SynchronizedMessage {
	final Long value;

	public LongMessage(Long value) {
		this.value = value;
	}
}
