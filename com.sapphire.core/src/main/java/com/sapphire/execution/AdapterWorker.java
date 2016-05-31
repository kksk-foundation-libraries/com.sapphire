package com.sapphire.execution;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public final class AdapterWorker<Message> extends Worker<Message> {
	private static final Timer TIMER = new Timer("StandbyAdapterChecker", true);

	private final BlockingQueue<Adapter<Message>> ready;
	private final BlockingQueue<Adapter<Message>> standby;
	private final Engine engine;

	public Engine getEngine() {
		return engine;
	}

	public AdapterWorker(String engineName, int workerThreads, int queueSize, int adapterMaxCount, long checkInterval) {
		this(new Engine(engineName, workerThreads, queueSize), queueSize, adapterMaxCount, checkInterval, false);
	}

	public AdapterWorker(String engineName, int workerThreads, int queueSize, int adapterMaxCount, long checkInterval, boolean sync) {
		this(new Engine(engineName, workerThreads, queueSize), queueSize, adapterMaxCount, checkInterval, sync);
	}

	public AdapterWorker(Engine engine, int queueSize, int adapterMaxCount, long checkInterval) {
		this(engine, queueSize, adapterMaxCount, checkInterval, false);
	}
	public AdapterWorker(Engine engine, int queueSize, int adapterMaxCount, long checkInterval, boolean sync) {
		super(engine, queueSize, sync);
		this.engine = engine;
		ready = new ArrayBlockingQueue<>(adapterMaxCount);
		standby = new ArrayBlockingQueue<>(adapterMaxCount);
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				int size = standby.toArray().length;
				for (int i = 0; i < size; i++) {
					Adapter<Message> adapter;
					try {
						adapter = standby.take();
						if (adapter.checkEnabled()) {
							ready.put(adapter);
						} else {
							standby.put(adapter);
						}
					} catch (InterruptedException e) {
						break;
					}
				}
			}
		};
		TIMER.schedule(task, 0, checkInterval);
	}

	public void addAdapter(Adapter<Message> adapter) {
		if (adapter == null)
			return;
		try {
			standby.put(adapter);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public final boolean process(Message message) {
		for (;;) {
			try {
				Adapter<Message> adapter = ready.take();
				if (adapter.enabled()) {
					adapter.execute(message);
					ready.put(adapter);
					break;
				}
				standby.put(adapter);
			} catch (InterruptedException e) {
				return false;
			}
		}
		return true;
	}
}
