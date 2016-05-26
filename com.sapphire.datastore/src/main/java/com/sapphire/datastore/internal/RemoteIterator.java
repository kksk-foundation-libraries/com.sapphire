package com.sapphire.datastore.internal;

import java.util.Map.Entry;

import com.sapphire.datastore.SchemeIterator;

class RemoteIterator implements SchemeIterator {
	// TODO

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public Entry<byte[], byte[]> next() {
		return null;
	}

	@Override
	public void close() throws Exception {
	}

	@Override
	public void seek(byte[] key) {
	}

	@Override
	public void seekToFirst() {
	}

	@Override
	public Entry<byte[], byte[]> peekNext() {
		return null;
	}

	@Override
	public boolean hasPrev() {
		return false;
	}

	@Override
	public Entry<byte[], byte[]> prev() {
		return null;
	}

	@Override
	public Entry<byte[], byte[]> peekPrev() {
		return null;
	}

	@Override
	public void seekToLast() {
	}

}
