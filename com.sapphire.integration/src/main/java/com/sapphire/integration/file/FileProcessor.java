package com.sapphire.integration.file;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import com.sapphire.execution.Adapter;
import com.sapphire.execution.AdapterWorker;
import com.sapphire.execution.Engine;

public abstract class FileProcessor<Message> implements Adapter<File>, DirectoryEventHandler {
	private final AdapterWorker<Message> adapterWorker;
	private final AdapterWorker<File> fileAdapterWorker;
	public static final Engine ENGINE = new Engine("FileProcessorEngine", 100, 1000);

	public FileProcessor(AdapterWorker<Message> adapterWorker) {
		this(adapterWorker, ENGINE);
	}

	public FileProcessor(AdapterWorker<Message> adapterWorker, Engine engine) {
		this.adapterWorker = adapterWorker;
		fileAdapterWorker = new AdapterWorker<>(engine, 1000, 1000, 1000, true);
		fileAdapterWorker.addAdapter(this);
	}

	@Override
	public final boolean enabled() {
		return true;
	}

	@Override
	public final boolean checkEnabled() {
		return true;
	}

	@Override
	public final void execute(File message) {
		try (MessageIterator<Message> iterator = iterator(message);) {
			while (iterator.hasNext()) {
				Message msg = iterator.next();
				adapterWorker.execute(msg);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public final void addTarget(String directory, String fileName) {
		DirectoryEventListener.instance.registerHandler(this, directory, fileName, true, true, false);
	}

	@Override
	public final void handle(Integer handlerId, DirectoryEvent event) {
		Throwable throwable = null;
		try {
			beforeExecute(event.file);
			fileAdapterWorker.execute(event.file);
		} catch (RuntimeException e) {
			throwable = e;
		} catch (InterruptedException e) {
			throwable = e;
		} finally {
			afterExecute(event.file, throwable);
		}
	}

	public static abstract class MessageIterator<Message> implements Iterator<Message>, AutoCloseable {
	}

	protected abstract MessageIterator<Message> iterator(File file) throws IOException;

	protected void beforeExecute(File file) {
		// if need...
	}

	protected void afterExecute(File file, Throwable throwable) {
		// if need...
	}
}
