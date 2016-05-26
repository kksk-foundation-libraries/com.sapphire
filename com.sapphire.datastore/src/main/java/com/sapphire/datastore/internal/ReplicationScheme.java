package com.sapphire.datastore.internal;

import static java.util.Arrays.asList;

import java.util.Collections;
import java.util.List;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeBatch;
import com.sapphire.datastore.SchemeEventListener;
import com.sapphire.datastore.SchemeIterator;

class ReplicationScheme extends Scheme {
	private final List<Scheme> replications;

	public ReplicationScheme(Scheme... replications) {
		this(asList(replications));
	}

	public ReplicationScheme(List<Scheme> replications) {
		this.replications = Collections.unmodifiableList(replications);
	}

	@Override
	public void close() throws Exception {
		for (Scheme replication : replications) {
			replication.close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		return replications.get(0).get(key);
	}

	@Override
	public SchemeIterator iterator() {
		return replications.get(0).iterator();
	}

	@Override
	public void put(byte[] key, byte[] value) {
		if (listen) {
			byte[] index = BinaryIndexGenerator.generateIndex();
			for (SchemeEventListener listener : listeners) {
				listener.put(index, key, value);
			}
		}
		for (Scheme replication : replications) {
			replication.put(key, value);
		}
	}

	@Override
	public void delete(byte[] key) {
		if (listen) {
			byte[] index = BinaryIndexGenerator.generateIndex();
			for (SchemeEventListener listener : listeners) {
				listener.delete(index, key);
			}
		}
		for (Scheme replication : replications) {
			replication.delete(key);
		}
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		boolean result = true;
		for (Scheme replication : replications) {
			if (!replication.putIfAbsent(key, value)) {
				result = false;
			}
		}
		return result;
	}

	@Override
	public void updateAll(SchemeBatch schemeBatch) {
		for (Scheme replication : replications) {
			replication.updateAll(new SchemeBatch(schemeBatch));
		}
	}
}
