package com.sapphire.datastore;

import java.util.Iterator;
import java.util.Map.Entry;

public interface SchemeIterator extends Iterator<Entry<byte[], byte[]>>, AutoCloseable {
	void seek(byte[] key);

	void seekToFirst();

	Entry<byte[], byte[]> peekNext();

	boolean hasPrev();

	Entry<byte[], byte[]> prev();

	Entry<byte[], byte[]> peekPrev();

	void seekToLast();
}
