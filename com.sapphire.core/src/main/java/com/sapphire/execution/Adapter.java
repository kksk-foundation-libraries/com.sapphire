package com.sapphire.execution;

public interface Adapter<Message> {
	boolean enabled();

	boolean checkEnabled();

	void execute(Message message);
}
