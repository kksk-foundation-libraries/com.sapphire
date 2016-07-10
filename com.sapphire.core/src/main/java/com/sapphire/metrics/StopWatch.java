package com.sapphire.metrics;

import java.lang.management.ManagementFactory;

public class StopWatch {
	private long start;
	private long stop;
	private long cpuStart;
	private long cpuStop;

	@SuppressWarnings("restriction")
	private final com.sun.management.OperatingSystemMXBean mxbean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

	@SuppressWarnings("restriction")
	public void start() {
		cpuStart = mxbean.getProcessCpuTime();
		start = System.nanoTime();
	}

	@SuppressWarnings("restriction")
	public void stop() {
		cpuStop = mxbean.getProcessCpuTime();
		stop = System.nanoTime();
		System.out.println(String.format("elapsed:[%,d], cpu:[%,d], cpu%%:[%,4.2f]", stop - start, cpuStop - cpuStart, 100 * (double) (cpuStop - cpuStart) / (double) (stop - start)));
	}
}
