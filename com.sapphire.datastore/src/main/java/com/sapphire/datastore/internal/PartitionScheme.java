package com.sapphire.datastore.internal;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeBatch;
import com.sapphire.datastore.SchemeEventListener;
import com.sapphire.datastore.SchemeIterator;

class PartitionScheme extends Scheme {
	private final List<Scheme> partitions;
	private final int count;

	private Scheme getPartition(byte[] key) {
		int hash = Arrays.hashCode(key);
		return partitions.get(hash % count);
	}

	public PartitionScheme(Scheme... partitions) {
		this(asList(partitions));
	}

	public PartitionScheme(List<Scheme> partitions) {
		this.partitions = Collections.unmodifiableList(partitions);
		count = this.partitions.size();
	}

	@Override
	public void close() throws Exception {
		for (Scheme partition : partitions) {
			partition.close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		return getPartition(key).get(key);
	}

	@Override
	public SchemeIterator iterator() {
		List<SchemeIterator> list = new ArrayList<>(count);
		for (Scheme partition : partitions) {
			list.add(partition.iterator());
		}
		return new MergeIterator(list);
	}

	@Override
	public void put(byte[] key, byte[] value) {
		if (listen) {
			byte[] index = BinaryIndexGenerator.generateIndex();
			for (SchemeEventListener listener : listeners) {
				listener.put(index, key, value);
			}
		}
		getPartition(key).put(key, value);
	}

	@Override
	public void delete(byte[] key) {
		if (listen) {
			byte[] index = BinaryIndexGenerator.generateIndex();
			for (SchemeEventListener listener : listeners) {
				listener.delete(index, key);
			}
		}
		getPartition(key).delete(key);
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		return getPartition(key).putIfAbsent(key, value);
	}

	@Override
	public void updateAll(SchemeBatch schemeBatch) {
		SchemeBatch[] schemeBatchs = new SchemeBatch[count];
		for (int i = 0; i < count; i++) {
			schemeBatchs[i] = new SchemeBatch();
		}
		for (Entry<byte[], byte[]> entry : schemeBatch.updates) {
			int hash = Arrays.hashCode(entry.getKey());
			schemeBatchs[hash % count].put(entry.getKey(), entry.getValue());
		}
		for (int i = 0; i < count; i++) {
			Scheme partition = partitions.get(i);
			partition.updateAll(schemeBatchs[i]);
		}
		schemeBatch.updates.clear();
	}
}
