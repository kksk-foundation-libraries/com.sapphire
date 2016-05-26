package com.sapphire.datastore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import com.sapphire.datastore.internal.BinaryEntry;

public final class SchemeBatch {
	public final List<Entry<byte[], byte[]>> updates = new ArrayList<>();

	public SchemeBatch() {
	}

	public SchemeBatch(SchemeBatch batch) {
		updates.addAll(batch.updates);
	}

	public SchemeBatch put(byte[] key, byte[] value) {
		updates.add(new BinaryEntry(key, value));
		return this;
	}

	public SchemeBatch delete(byte[] key) {
		updates.add(new BinaryEntry(key, null));
		return this;
	}
}
