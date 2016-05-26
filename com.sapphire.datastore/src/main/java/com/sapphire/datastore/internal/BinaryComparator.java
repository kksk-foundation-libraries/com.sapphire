package com.sapphire.datastore.internal;

import java.util.Comparator;

class BinaryComparator implements Comparator<byte[]> {
	@Override
	public int compare(byte[] o1, byte[] o2) {
		if (o1 == null) {
			if (o2 == null) {
				return 0;
			} else {
				return 1;
			}
		} else if (o2 == null) {
			return -1;
		}
		for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
			int compared = compare(o1[i], o2[i]);
			if (compared == 0)
				continue;
			return compared;
		}
		return compare(o1.length, o2.length);
	}

	private int compare(byte b1, byte b2) {
		int i1 = b1 & 0xff;
		int i2 = b2 & 0xff;
		return compare(i1, i2);
	}

	private int compare(int i1, int i2) {
		return i1 == i2 ? 0 : (i1 > i2 ? -1 : 1);
	}
}
