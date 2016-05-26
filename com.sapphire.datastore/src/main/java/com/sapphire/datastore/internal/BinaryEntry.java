package com.sapphire.datastore.internal;

import java.util.Map.Entry;

public class BinaryEntry implements Entry<byte[], byte[]> {
	private final byte[] key;
	private final byte[] value;

	public BinaryEntry(byte[] key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public byte[] getKey() {
		return key;
	}

	@Override
	public byte[] getValue() {
		return value;
	}

	@Override
	public byte[] setValue(byte[] value) {
		throw new UnsupportedOperationException();
	}

}
