package com.sapphire.execution;

import java.util.concurrent.ArrayBlockingQueue;

class MessageBlockingQueue<Message> extends ArrayBlockingQueue<MessageHolder<Message>> {
	/** serialVersionUID */
	private static final long serialVersionUID = -8587032466149641031L;

	MessageBlockingQueue(int capacity) {
		super(capacity);
	}
}
