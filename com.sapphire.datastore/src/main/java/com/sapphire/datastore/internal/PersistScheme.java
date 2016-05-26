package com.sapphire.datastore.internal;

import org.iq80.leveldb.DB;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeIterator;

class PersistScheme extends Scheme {
	private final DB db;

	public PersistScheme(DB db) {
		this.db = db;
	}

	@Override
	public void close() throws Exception {
		db.close();
	}

	@Override
	public byte[] get(byte[] key) {
		return db.get(key);
	}

	@Override
	public void put(byte[] key, byte[] value) {
		db.put(key, value);
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		if (get(key) != null) {
			return false;
		}
		put(key, value);
		return true;
	}

	@Override
	public void delete(byte[] key) {
		db.delete(key);
	}

	@Override
	public SchemeIterator iterator() {
		return new PersistIterator(db.iterator());
	}

}
