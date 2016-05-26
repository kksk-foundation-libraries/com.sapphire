package com.sapphire.datastore.internal;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeEventListener;
import com.sapphire.datastore.SchemeIterator;

class JournalScheme extends Scheme {
	protected final Scheme indexScheme;
	protected final Scheme journalScheme;

	public JournalScheme(Scheme indexScheme, Scheme journalScheme) {
		this.indexScheme = indexScheme;
		this.journalScheme = journalScheme;
	}

	@Override
	public void close() throws Exception {
		indexScheme.close();
		journalScheme.close();
		for (SchemeEventListener listener : listeners) {
			listener.close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		byte[] index = indexScheme.get(key);
		if (index == null)
			return null;
		return journalScheme.get(index);
	}

	@Override
	public void put(byte[] key, byte[] value) {
		byte[] index = BinaryIndexGenerator.generateIndex();
		byte[] last = indexScheme.get(key);
		if (listen) {
			for (SchemeEventListener listener : listeners) {
				listener.put(index, key, value);
			}
		}
		journalScheme.put(index, value);
		indexScheme.put(key, index);
		if (last != null) {
			journalScheme.delete(last);
		}
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		if (indexScheme.get(key) != null)
			return false;
		put(key, value);
		return true;
	}

	@Override
	public void delete(byte[] key) {
		byte[] index = BinaryIndexGenerator.generateIndex();
		byte[] last = indexScheme.get(key);
		if (listen) {
			for (SchemeEventListener listener : listeners) {
				listener.delete(index, key);
			}
		}
		indexScheme.delete(key);
		if (last != null) {
			journalScheme.delete(last);
		}
	}

	@Override
	public SchemeIterator iterator() {
		return new JournalIterator(this);
	}

}
