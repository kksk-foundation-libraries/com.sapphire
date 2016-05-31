package com.sapphire.execution;

public abstract class Worker<Message> implements Runnable {
	private final boolean sync;
	private final Engine engine;
	private final MessageBlockingQueue<Message> queue;

	public Worker(Engine engine, int queueSize) {
		this(engine, queueSize, false);
	}

	public Worker(Engine engine, int queueSize, boolean sync) {
		this.engine = engine;
		this.queue = new MessageBlockingQueue<>(queueSize);
		this.sync = sync;
	}

	public final void execute(Message message) throws InterruptedException {
		MessageHolder<Message> holder = new MessageHolder<Message>(message);
		if (sync) {
			holder.lock.lock();
		}
		queue.put(holder);
		engine.execute(this);
		if (sync) {
			holder.condition.await();
			holder.lock.unlock();
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
