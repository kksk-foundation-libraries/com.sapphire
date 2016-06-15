package com.sapphire.integration.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

public final class DirectoryEventListener {
	public static final DirectoryEventListener instance = new DirectoryEventListener();
	private final AtomicInteger handlerIdSequence = new AtomicInteger(1);
	private final ConcurrentMap<Integer, WatchCondition> handlerMap = new ConcurrentHashMap<>();
	private final Lock lock = new ReentrantLock();

	private static final class WatchCondition {
		protected final DirectoryEventHandler handler;
		protected final String directory;
		protected final Pattern pattern;
		protected final boolean create;
		protected final boolean modify;
		protected final boolean remove;
		protected WatchKey watchKey = null;

		public WatchCondition(DirectoryEventHandler handler, String directory, String fileName, boolean create, boolean modify, boolean remove) {
			this.handler = handler;
			this.directory = directory;
			this.pattern = Pattern.compile(fileName);
			this.create = create;
			this.modify = modify;
			this.remove = remove;
		}
	}

	private DirectoryEventListener() {
	}

	public Integer registerHandler(DirectoryEventHandler handler, String directory, String fileName, boolean create, boolean modify, boolean remove) {
		return registerHandler(new WatchCondition(handler, directory, fileName, create, modify, remove));
	}

	private Integer registerHandler(WatchCondition watchCondition) {
		lock.lock();
		Integer handlerId = handlerIdSequence.getAndIncrement();
		handlerMap.put(handlerId, watchCondition);
		lock.unlock();
		return handlerId;
	}

	public void unregisterHandler(Integer handlerId) {
		lock.lock();
		handlerMap.remove(handlerId);
		lock.unlock();
	}

	private Thread thread = null;

	private static final ThreadGroup THREAD_GROUP = new ThreadGroup("DirectoryEventListener");
	private static final Runnable COMMAND = new Runnable() {
		@Override
		public void run() {
			final ConcurrentMap<Integer, WatchCondition> handlerMap = DirectoryEventListener.instance.handlerMap;
			FileSystem fileSystem = null;
			for (Entry<Integer, WatchCondition> entry : handlerMap.entrySet()) {
				Path path = new File(entry.getValue().directory).toPath();
				fileSystem = path.getFileSystem();
				break;
			}
			if (fileSystem != null) {
				try (WatchService watcher = fileSystem.newWatchService()) {
					for (Entry<Integer, WatchCondition> entry : handlerMap.entrySet()) {
						WatchCondition condition = entry.getValue();
						Path path = new File(condition.directory).toPath();

						List<Kind<Path>> kinds = new ArrayList<>();
						if (condition.create) {
							kinds.add(StandardWatchEventKinds.ENTRY_CREATE);
						} else if (condition.modify) {
							kinds.add(StandardWatchEventKinds.ENTRY_MODIFY);
						} else {
							kinds.add(StandardWatchEventKinds.ENTRY_DELETE);
						}
						condition.watchKey = path.register(watcher, kinds.toArray(new Kind[kinds.size()]));
					}
					watch(watcher, handlerMap);
				} catch (IOException e) {
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
				}
			}
		}

		private void watch(WatchService watcher, ConcurrentMap<Integer, WatchCondition> handlerMap) throws InterruptedException {
			while (true) {
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
				WatchKey detected = watcher.poll();
				if (detected == null) {
					Thread.sleep(10);
					continue;
				}
				for (Entry<Integer, WatchCondition> entry : handlerMap.entrySet()) {
					WatchCondition condition = entry.getValue();
					WatchKey watchKey = condition.watchKey;
					if (watchKey != null && watchKey.isValid()) {
						if (detected.equals(watchKey)) {
							for (WatchEvent<?> event : detected.pollEvents()) {
								Path path = (Path) event.context();
								if (condition.pattern.matcher(path.toFile().getName()).matches()) {
									if (condition.create && event.kind().equals(StandardWatchEventKinds.ENTRY_CREATE)) {
										condition.handler.handle(entry.getKey(), new DirectoryEvent(DirectoryEventType.CREATE, path.toFile()));
									} else if (condition.modify && event.kind().equals(StandardWatchEventKinds.ENTRY_MODIFY)) {
										condition.handler.handle(entry.getKey(), new DirectoryEvent(DirectoryEventType.MODIFY, path.toFile()));
									} else if (condition.remove && event.kind().equals(StandardWatchEventKinds.ENTRY_DELETE)) {
										condition.handler.handle(entry.getKey(), new DirectoryEvent(DirectoryEventType.REMOVE, path.toFile()));
									}
								}

							}
						}
					}
				}
				detected.reset();
			}
		}
	};

	public void listen() {
		lock.lock();
		if (thread == null) {
			thread = new Thread(THREAD_GROUP, COMMAND, "DirectoryEventListener");
			thread.setDaemon(true);
			thread.start();
		} else if (!thread.isAlive()) {
			thread.start();
		}
		lock.unlock();
	}

	public void stopListen() {
		lock.lock();
		if (thread != null && thread.isAlive()) {
			thread.interrupt();
		}
		lock.unlock();
	}
}
