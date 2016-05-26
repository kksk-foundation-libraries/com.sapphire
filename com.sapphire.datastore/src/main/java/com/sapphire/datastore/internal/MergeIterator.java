package com.sapphire.datastore.internal;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

import com.sapphire.datastore.SchemeIterator;

class MergeIterator implements SchemeIterator {
	private final List<SchemeIterator> iterators;
	private final int count;
	private int direction = 0;
	private Object[] nexts;
	private static final Comparator<byte[]> BINARY_COMPARATOR = new BinaryComparator();

	public MergeIterator(List<SchemeIterator> iterators) {
		this.iterators = Collections.unmodifiableList(iterators);
		count = this.iterators.size();
		nexts = new Object[count];
	}

	@Override
	public boolean hasNext() {
		if (direction > 0) {
			for (Object value : nexts) {
				if (value != null) {
					return true;
				}
			}
		}
		for (SchemeIterator iterator : iterators) {
			if (iterator.hasNext()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Entry<byte[], byte[]> next() {
		if (direction == 1) {
			for (int i = 0; i < count; i++) {
				if (nexts[i] == null && iterators.get(i).hasNext()) {
					nexts[i] = iterators.get(i).next();
				}
			}
		} else {
			for (int i = 0; i < count; i++) {
				if (iterators.get(i).hasNext()) {
					nexts[i] = iterators.get(i).next();
				} else {
					nexts[i] = null;
				}
			}
		}
		direction = 1;
		return getMin();
	}

	@SuppressWarnings("unchecked")
	private Entry<byte[], byte[]> getMin() {
		int index = -1;
		Entry<byte[], byte[]> min = null;
		for (int i = 0; i < count; i++) {
			Object o = nexts[i];
			if (o != null) {
				Entry<byte[], byte[]> v = (Entry<byte[], byte[]>) o;
				if (min == null) {
					min = v;
					index = i;
				} else {
					if (BINARY_COMPARATOR.compare(min.getKey(), v.getKey()) < 0) {
						min = v;
						index = i;
					}
				}
			}
		}
		if (index != -1) {
			nexts[index] = null;
		}
		return min;
	}

	@Override
	public void close() throws Exception {
		for (SchemeIterator iterator : iterators) {
			iterator.close();
		}
	}

	@Override
	public void seek(byte[] key) {
		for (SchemeIterator iterator : iterators) {
			iterator.seek(key);
		}
	}

	@Override
	public void seekToFirst() {
		for (SchemeIterator iterator : iterators) {
			iterator.seekToFirst();
		}
	}

	@Override
	public Entry<byte[], byte[]> peekNext() {
		if (direction == 2) {
			for (int i = 0; i < count; i++) {
				if (nexts[i] == null && iterators.get(i).hasNext()) {
					nexts[i] = iterators.get(i).peekNext();
				}
			}
		} else {
			for (int i = 0; i < count; i++) {
				if (iterators.get(i).hasNext()) {
					nexts[i] = iterators.get(i).peekNext();
				} else {
					nexts[i] = null;
				}
			}
		}
		direction = 2;
		return getMin();
	}

	@Override
	public boolean hasPrev() {
		if (direction < 0) {
			for (Object value : nexts) {
				if (value != null) {
					return true;
				}
			}
		}
		for (SchemeIterator iterator : iterators) {
			if (iterator.hasPrev()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public Entry<byte[], byte[]> prev() {
		if (direction == -1) {
			for (int i = 0; i < count; i++) {
				if (nexts[i] == null && iterators.get(i).hasPrev()) {
					nexts[i] = iterators.get(i).prev();
				}
			}
		} else {
			for (int i = 0; i < count; i++) {
				if (iterators.get(i).hasPrev()) {
					nexts[i] = iterators.get(i).prev();
				} else {
					nexts[i] = null;
				}
			}
		}
		direction = -1;
		return getMax();
	}

	@SuppressWarnings("unchecked")
	private Entry<byte[], byte[]> getMax() {
		int index = -1;
		Entry<byte[], byte[]> max = null;
		for (int i = 0; i < count; i++) {
			Object o = nexts[i];
			if (o != null) {
				Entry<byte[], byte[]> v = (Entry<byte[], byte[]>) o;
				if (max == null) {
					max = v;
					index = i;
				} else {
					if (BINARY_COMPARATOR.compare(max.getKey(), v.getKey()) > 0) {
						max = v;
						index = i;
					}
				}
			}
		}
		if (index != -1) {
			nexts[index] = null;
		}
		return max;
	}

	@Override
	public Entry<byte[], byte[]> peekPrev() {
		if (direction == -2) {
			for (int i = 0; i < count; i++) {
				if (nexts[i] == null && iterators.get(i).hasPrev()) {
					nexts[i] = iterators.get(i).peekPrev();
				}
			}
		} else {
			for (int i = 0; i < count; i++) {
				if (iterators.get(i).hasPrev()) {
					nexts[i] = iterators.get(i).peekPrev();
				} else {
					nexts[i] = null;
				}
			}
		}
		direction = -2;
		return getMax();
	}

	@Override
	public void seekToLast() {
		for (SchemeIterator iterator : iterators) {
			iterator.seekToLast();
		}
	}

}
