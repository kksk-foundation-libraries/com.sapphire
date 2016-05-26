package com.sapphire.execution;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Engine {
	private static final RejectedExecutionHandler HANDLER = new RejectedExecutionHandler() {
		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			executor.execute(r);
		}
	};

	private final BlockingQueue<Runnable> workerQueue;
	private final NamedThreadFactory threadFactory;
	private final int workerThreads;
	private PausableThreadPoolExecutor threadPool = null;

	public Engine(String engineName, int workerThreads, int queueSize) {
		this.workerThreads = workerThreads;
		this.workerQueue = new ArrayBlockingQueue<>(queueSize);
		this.threadFactory = new NamedThreadFactory(engineName);
	}

	public void start() {
		if (threadPool != null)
			throw new RuntimeException("Already started.");
		threadPool = new PausableThreadPoolExecutor(threadFactory, workerThreads, workerQueue);
	}

	public void stop() {
		stop(false);
	}

	public void stop(boolean force) {
		if (threadPool == null)
			throw new RuntimeException("Not yet started.");
		if (force) {
			threadPool.shutdownNow();
		} else {
			threadPool.shutdown();
		}
		try {
			while (!threadPool.awaitTermination(10, TimeUnit.MILLISECONDS)) {
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public void execute(Runnable command) {
		if (threadPool == null)
			throw new RuntimeException("Not yet started.");
		threadPool.execute(command);
	}

	public void pause() {
		if (threadPool == null)
			throw new RuntimeException("Not yet started.");
		if (!threadPool.isPaused)
			threadPool.pause();
	}

	public void resume() {
		if (threadPool == null)
			throw new RuntimeException("Not yet started.");
		if (threadPool.isPaused)
			threadPool.resume();
	}

	private static class NamedThreadFactory implements ThreadFactory {
		private final String name;
		private final ThreadGroup threadGroup;
		private final AtomicInteger threadCount = new AtomicInteger();

		public NamedThreadFactory(String name) {
			this.name = name;
			threadGroup = new ThreadGroup(name);
		}

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(threadGroup, r, name + "-" + String.format("%05d", threadCount.incrementAndGet()));
		}
	}

	private static class PausableThreadPoolExecutor extends ThreadPoolExecutor {
		private boolean isPaused;
		private ReentrantLock pauseLock = new ReentrantLock();
		private Condition unpaused = pauseLock.newCondition();

		public PausableThreadPoolExecutor(ThreadFactory threadFactory, int workerThreads, BlockingQueue<Runnable> workerQueue) {
			super(workerThreads, workerThreads, 1, TimeUnit.SECONDS, workerQueue, threadFactory, HANDLER);
		}

		protected void beforeExecute(Thread t, Runnable r) {
			super.beforeExecute(t, r);
			pauseLock.lock();
			try {
				while (isPaused)
					unpaused.await();
			} catch (InterruptedException ie) {
				t.interrupt();
			} finally {
				pauseLock.unlock();
			}
		}

		public void pause() {
			pauseLock.lock();
			try {
				isPaused = true;
			} finally {
				pauseLock.unlock();
			}
		}

		public void resume() {
			pauseLock.lock();
			try {
				isPaused = false;
				unpaused.signalAll();
			} finally {
				pauseLock.unlock();
			}
		}
	}
}
