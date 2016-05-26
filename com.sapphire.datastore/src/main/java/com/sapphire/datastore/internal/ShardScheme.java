package com.sapphire.datastore.internal;

import static java.util.Arrays.asList;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.sapphire.datastore.Scheme;
import com.sapphire.datastore.SchemeBatch;
import com.sapphire.datastore.SchemeEventListener;
import com.sapphire.datastore.SchemeIterator;

import java.util.SortedMap;
import java.util.TreeMap;

class ShardScheme extends Scheme {
	private final List<Shard> shards;
	private final int count;
	private final SortedMap<Integer, Shard> circle;

	private static final MessageDigest MD5;
	static {
		MessageDigest md5;
		try {
			md5 = MessageDigest.getInstance("md5");
		} catch (NoSuchAlgorithmException e) {
			md5 = null;
		}
		MD5 = md5;
	}

	private Shard getShard(byte[] key) {
		int hash = Arrays.hashCode(MD5.digest(key));
		if (!circle.containsKey(hash)) {
			SortedMap<Integer, Shard> tailMap = circle.tailMap(hash);
			hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
		}
		return circle.get(hash);
	}

	public ShardScheme(int replicas, Shard... shards) {
		this(replicas, asList(shards));
	}

	public ShardScheme(int replicas, List<Shard> shards) {
		this.shards = Collections.unmodifiableList(shards);
		count = this.shards.size();
		circle = new TreeMap<>();
		for (int node = 0; node < count; node++) {
			String nodeName = shards.get(node).getName() + "-";
			for (int i = 0; i < replicas; i++) {
				String shardName = nodeName + i;
				circle.put(Arrays.hashCode(shardName.getBytes()), this.shards.get(node));
			}
		}
	}

	@Override
	public void close() throws Exception {
		for (Shard shard : shards) {
			shard.getScheme().close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		return getShard(key).getScheme().get(key);
	}

	@Override
	public SchemeIterator iterator() {
		List<SchemeIterator> list = new ArrayList<>(count);
		for (Shard shard : shards) {
			list.add(shard.getScheme().iterator());
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
		getShard(key).getScheme().put(key, value);
	}

	@Override
	public void delete(byte[] key) {
		if (listen) {
			byte[] index = BinaryIndexGenerator.generateIndex();
			for (SchemeEventListener listener : listeners) {
				listener.delete(index, key);
			}
		}
		getShard(key).getScheme().delete(key);
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		return getShard(key).getScheme().putIfAbsent(key, value);
	}

	@Override
	public void updateAll(SchemeBatch schemeBatch) {
		Map<Integer, SchemeBatch> schemeBatchs = new HashMap<>();
		for (Integer index : circle.keySet()) {
			schemeBatchs.put(index, new SchemeBatch());
		}
		for (Entry<byte[], byte[]> entry : schemeBatch.updates) {
			int hash = Arrays.hashCode(MD5.digest(entry.getKey()));
			if (!circle.containsKey(hash)) {
				SortedMap<Integer, Shard> tailMap = circle.tailMap(hash);
				hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
			}
			Integer index = hash;
			schemeBatchs.get(index).put(entry.getKey(), entry.getValue());
		}
		for (Integer index : circle.keySet()) {
			Scheme scheme = circle.get(index).getScheme();
			scheme.updateAll(schemeBatchs.get(index));
		}
		schemeBatch.updates.clear();
	}

}
