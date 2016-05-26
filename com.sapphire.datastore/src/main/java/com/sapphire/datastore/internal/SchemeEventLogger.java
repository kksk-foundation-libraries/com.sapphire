package com.sapphire.datastore.internal;

import java.nio.ByteBuffer;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeEventListener;

class SchemeEventLogger implements SchemeEventListener {
	private final Scheme logScheme;

	public SchemeEventLogger(Scheme logScheme) {
		this.logScheme = logScheme;
	}

	@Override
	public void put(byte[] eventId, byte[] key, byte[] value) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + key.length + Integer.BYTES + value.length);
		buffer.putInt(key.length);
		buffer.put(key);
		buffer.putInt(value.length);
		buffer.put(value);
		logScheme.put(eventId, buffer.array());
	}

	@Override
	public void delete(byte[] eventId, byte[] key) {
		ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + key.length + Integer.BYTES);
		buffer.putInt(key.length);
		buffer.put(key);
		buffer.putInt(0);
		logScheme.put(eventId, buffer.array());
	}

	@Override
	public void close() throws Exception {
		logScheme.close();
	}

}
