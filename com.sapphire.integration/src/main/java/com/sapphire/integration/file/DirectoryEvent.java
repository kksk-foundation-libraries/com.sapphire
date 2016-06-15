package com.sapphire.integration.file;

import java.io.File;

public final class DirectoryEvent {
	protected final DirectoryEventType type;
	protected final File file;

	public DirectoryEvent(DirectoryEventType type, File file) {
		this.type = type;
		this.file = file;
	}
}
