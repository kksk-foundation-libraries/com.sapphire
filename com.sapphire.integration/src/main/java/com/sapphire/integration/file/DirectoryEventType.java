package com.sapphire.integration.file;

public enum DirectoryEventType {
	CREATE(1), MODIFY(2), REMOVE(3);

	protected final int eventType;

	private DirectoryEventType(int eventType) {
		this.eventType = eventType;
	}
}
