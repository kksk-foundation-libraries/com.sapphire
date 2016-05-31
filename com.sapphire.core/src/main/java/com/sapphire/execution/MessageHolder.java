package com.sapphire.execution;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class MessageHolder<Message> {
	final Lock lock = new ReentrantLock();
	final Condition condition = lock.newCondition();
	final Message message;

	MessageHolder(Message message) {
		this.message = message;
	}
}
