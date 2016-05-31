package com.sapphire.execution;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public abstract class SynchronizedMessage {
	boolean locked = false;
	final ReentrantLock lock = new ReentrantLock();
	final Condition condition = lock.newCondition();
}
