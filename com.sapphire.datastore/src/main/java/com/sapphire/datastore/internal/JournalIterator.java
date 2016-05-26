package com.sapphire.datastore.internal;

import java.util.Map.Entry;

import com.sapphire.datastore.SchemeIterator;

class JournalIterator implements SchemeIterator {
	private final JournalScheme scheme;
	private final SchemeIterator indexSchemeIterator;

	public JournalIterator(JournalScheme scheme) {
		this.scheme = scheme;
		indexSchemeIterator = scheme.indexScheme.iterator();
		seekToFirst();
	}

	@Override
	public boolean hasNext() {
		return indexSchemeIterator.hasNext();
	}

	private Entry<byte[], byte[]> lookup(byte[] key) {
		return new BinaryEntry(key, scheme.journalScheme.get(key));
	}

	@Override
	public Entry<byte[], byte[]> next() {
		return lookup(indexSchemeIterator.next().getKey());
	}

	@Override
	public void close() throws Exception {
		indexSchemeIterator.close();
	}

	@Override
	public void seek(byte[] key) {
		indexSchemeIterator.seek(key);
	}

	@Override
	public void seekToFirst() {
		indexSchemeIterator.seekToFirst();
	}

	@Override
	public Entry<byte[], byte[]> peekNext() {
		return lookup(indexSchemeIterator.peekNext().getKey());
	}

	@Override
	public boolean hasPrev() {
		return indexSchemeIterator.hasPrev();
	}

	@Override
	public Entry<byte[], byte[]> prev() {
		return lookup(indexSchemeIterator.prev().getKey());
	}

	@Override
	public Entry<byte[], byte[]> peekPrev() {
		return lookup(indexSchemeIterator.peekPrev().getKey());
	}

	@Override
	public void seekToLast() {
		indexSchemeIterator.seekToLast();
	}

}
