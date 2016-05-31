package com.sapphire.execution;

import java.util.concurrent.BlockingQueue;

public abstract class Worker<Message> implements Runnable {
	private final Engine engine;
	private final BlockingQueue<Message> queue;

	public Worker(Engine engine, BlockingQueue<Message> queue) {
		this.engine = engine;
		this.queue = queue;
	}

	public void execute(Message message) throws InterruptedException {
		queue.put(message);
		engine.execute(this);
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
		Message message = null;
		try {
			message = queue.take();
		} catch (InterruptedException e) {
			processingError(message, new RuntimeException(e));
			return;
		}
		try {
			if (!process(message))
				return;
		} catch (RuntimeException e) {
			processingError(message, e);
			return;
		}
		try {
			if (!postprocess())
				return;
		} catch (RuntimeException e) {
			processingError(message, e);
			return;
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
