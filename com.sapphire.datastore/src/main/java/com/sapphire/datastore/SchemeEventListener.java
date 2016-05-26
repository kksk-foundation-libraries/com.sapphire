package com.sapphire.datastore;

public interface SchemeEventListener extends AutoCloseable {
	void put(byte[] eventId, byte[] key, byte[] value);

	void delete(byte[] eventId, byte[] key);
}
