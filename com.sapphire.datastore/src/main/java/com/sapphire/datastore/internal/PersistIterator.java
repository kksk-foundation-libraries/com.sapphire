package com.sapphire.datastore.internal;

import java.util.Map.Entry;

import org.iq80.leveldb.DBIterator;

import com.sapphire.datastore.SchemeIterator;

class PersistIterator implements SchemeIterator {
	private final DBIterator iterator;

	public PersistIterator(DBIterator iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public Entry<byte[], byte[]> next() {
		return iterator.next();
	}

	@Override
	public void close() throws Exception {
		iterator.close();
	}

	@Override
	public void seek(byte[] key) {
		iterator.seek(key);
	}

	@Override
	public void seekToFirst() {
		iterator.seekToFirst();
	}

	@Override
	public Entry<byte[], byte[]> peekNext() {
		return iterator.peekNext();
	}

	@Override
	public boolean hasPrev() {
		return iterator.hasPrev();
	}

	@Override
	public Entry<byte[], byte[]> prev() {
		return iterator.prev();
	}

	@Override
	public Entry<byte[], byte[]> peekPrev() {
		return iterator.peekPrev();
	}

	@Override
	public void seekToLast() {
		iterator.seekToLast();
	}

}
