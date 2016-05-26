package com.sapphire.datastore.internal;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeIterator;

class TransactionalScheme extends Scheme {
	// TODO

	@Override
	public void close() throws Exception {
	}

	@Override
	public byte[] get(byte[] key) {
		return null;
	}

	@Override
	public void put(byte[] key, byte[] value) {
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		return false;
	}

	@Override
	public void delete(byte[] key) {
	}

	@Override
	public SchemeIterator iterator() {
		return null;
	}

}
