package com.sapphire.integration.file;

public interface DirectoryEventHandler {
	void handle(Integer handlerId, DirectoryEvent event);
}
