package com.sapphire.execution;

import java.util.concurrent.BlockingQueue;

public class SynchronizedAdapterWorker<Message extends SynchronizedMessage> extends AdapterWorker<Message> {

	public SynchronizedAdapterWorker(Engine engine, BlockingQueue<Message> queue, int adapterMaxCount, long checkInterval) {
		super(engine, queue, adapterMaxCount, checkInterval);
	}

	public SynchronizedAdapterWorker(String engineName, int workerThreads, int queueSize, int adapterMaxCount, long checkInterval) {
		super(engineName, workerThreads, queueSize, adapterMaxCount, checkInterval);
	}

	@Override
	public boolean process(Message message) {
		message.lock.lock();
		for (;;) {
			try {
				Adapter<Message> adapter = ready.take();
				if (adapter.enabled()) {
					adapter.execute(message);
					ready.put(adapter);
					message.locked = false;
					message.condition.signalAll();
					message.lock.unlock();
					break;
				}
				standby.put(adapter);
			} catch (InterruptedException e) {
				message.locked = false;
				message.condition.signalAll();
				message.lock.unlock();
				return false;
			}
		}
		return true;
	}

	@Override
	public void execute(Message message) throws InterruptedException {
		message.lock.lock();
		message.locked = true;
		super.execute(message);
		while (message.locked)
			message.condition.await();
		message.lock.unlock();
	}
}
