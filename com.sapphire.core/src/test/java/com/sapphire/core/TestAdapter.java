package com.sapphire.core;

import java.util.concurrent.atomic.AtomicLong;

import com.sapphire.execution.Adapter;

public class TestAdapter implements Adapter<Long> {
	private static final AtomicLong SEQUENCE = new AtomicLong();
	private final long sequence;
	private boolean enabled = false;
	public static final AtomicLong COUNTER = new AtomicLong();
	public static final AtomicLong ENABLED_COUNTER = new AtomicLong();
	public static final AtomicLong DISABLED_COUNTER = new AtomicLong();

	public TestAdapter() {
		this.sequence = SEQUENCE.getAndIncrement();
		this.enabled = sequence % 5 != 0;
		if (enabled) {
			ENABLED_COUNTER.getAndIncrement();
		} else {
			DISABLED_COUNTER.getAndIncrement();
		}
	}

	@Override
	public boolean enabled() {
		return enabled;
	}

	@Override
	public boolean checkEnabled() {
		if (!enabled && sequence % 2 == 0) {
			enabled = true;
			ENABLED_COUNTER.getAndIncrement();
			DISABLED_COUNTER.getAndDecrement();
		}
		return enabled;
	}

	@Override
	public void execute(Long message) {
		COUNTER.getAndIncrement();
	}

}
