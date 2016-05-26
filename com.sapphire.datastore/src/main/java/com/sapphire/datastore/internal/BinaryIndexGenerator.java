package com.sapphire.datastore.internal;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

final class BinaryIndexGenerator {
	private static final AtomicInteger INDEX = new AtomicInteger();
	private static final int MACHINE_ID;
	static {
		int machineId = Integer.valueOf(System.getProperty("machine.id", "0"));
		MACHINE_ID = machineId;
	}

	public static byte[] generateIndex() {
		ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + Integer.BYTES);
		buffer.order(ByteOrder.BIG_ENDIAN);
		if (INDEX.compareAndSet(Integer.MAX_VALUE, 0)) {
			sleep();
			buffer.putLong(System.currentTimeMillis());
			buffer.putInt(MACHINE_ID);
			buffer.putInt(INDEX.getAndIncrement());
		} else {
			buffer.putLong(System.currentTimeMillis());
			buffer.putInt(MACHINE_ID);
			buffer.putInt(INDEX.getAndIncrement());
		}
		return buffer.array();
	}

	private static void sleep() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
		}
	}
}
