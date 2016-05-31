package com.sapphire.execution;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class Worker<Message> implements Runnable {
	private final boolean sync;
	private final Engine engine;
	private final MessageBlockingQueue<Message> queue;
	private final BlockingQueue<MessageHolder<Message>> holderPool;

	public Worker(Engine engine, int queueSize) {
		this(engine, queueSize, false);
	}

	public Worker(Engine engine, int queueSize, boolean sync) {
		this.engine = engine;
		this.queue = new MessageBlockingQueue<>(queueSize);
		this.sync = sync;
		int size = 0;
		if (sync) {
			size = engine.workerThreads * 2;
		} else {
			size = Math.max(Math.min(10_000, queueSize / 2), engine.workerThreads * 2);
		}
		holderPool = new ArrayBlockingQueue<>(size);
		for (int i = 0; i < size; i++) {
			holderPool.add(new MessageHolder<Message>(null));
		}
	}

	private MessageHolder<Message> getHolder(Message message) {
		MessageHolder<Message> holder = holderPool.poll();
		if (holder == null) {
			holder = new MessageHolder<>(message);
		} else {
			holder.message = message;
		}
		return holder;
	}

	private void releaseHolder(MessageHolder<Message> holder) {
		if (holder != null)
			holderPool.offer(holder);
	}

	public final void execute(Message message) throws InterruptedException {
		MessageHolder<Message> holder = getHolder(message);
		if (sync) {
			holder.lock.lock();
		}
		queue.put(holder);
		engine.execute(this);
		if (sync) {
			holder.condition.await();
			holder.lock.unlock();
			releaseHolder(holder);
		}
	}

	@Override
	public final void run() {
		try {
			if (!preprocess())
				return;
		} catch (RuntimeException e) {
			processingError(null, e);
			return;
		}
		MessageHolder<Message> holder = null;
		Message message = null;
		try {
			holder = queue.take();
			if (sync) {
				holder.lock.lock();
			}
			message = holder.message;
		} catch (InterruptedException e) {
			processingError(message, new RuntimeException(e));
			unlock(holder);
			return;
		}
		try {
			if (!process(message)) {
				unlock(holder);
				return;
			}
		} catch (RuntimeException e) {
			processingError(message, e);
			unlock(holder);
			return;
		}
		try {
			if (!postprocess()) {
				unlock(holder);
				return;
			}
		} catch (RuntimeException e) {
			processingError(message, e);
			unlock(holder);
			return;
		}
		unlock(holder);
	}

	private void unlock(MessageHolder<Message> holder) {
		if (sync && holder != null) {
			holder.condition.signalAll();
			holder.lock.unlock();
		} else {
			releaseHolder(holder);
		}
	}

	public boolean preprocess() {
		return true;
	}

	public boolean process(Message message) {
		return true;
	}

	public boolean postprocess() {
		return true;
	}

	public void processingError(Message message, RuntimeException e) {
		// no op.
	}

}
