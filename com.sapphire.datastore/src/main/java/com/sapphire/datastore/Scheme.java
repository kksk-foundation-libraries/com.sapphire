package com.sapphire.datastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

public abstract class Scheme implements AutoCloseable {
	protected List<SchemeEventListener> listeners = new ArrayList<>();
	protected boolean listen = false;

	public abstract byte[] get(byte[] key);

	public abstract void put(byte[] key, byte[] value);

	public abstract boolean putIfAbsent(byte[] key, byte[] value);

	public abstract void delete(byte[] key);

	public abstract SchemeIterator iterator();

	public void addEventListener(SchemeEventListener... listeners) {
		this.listeners.addAll(Arrays.asList(listeners));
		listen = (this.listeners.size() > 0);
	}

	public void updateAll(SchemeBatch schemeBatch) {
		for (Entry<byte[], byte[]> entry : schemeBatch.updates) {
			if (entry.getValue() != null) {
				put(entry.getKey(), entry.getValue());
			} else {
				delete(entry.getKey());
			}
		}
	}
}
