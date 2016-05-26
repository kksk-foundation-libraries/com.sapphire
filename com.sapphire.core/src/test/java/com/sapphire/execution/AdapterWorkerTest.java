package com.sapphire.execution;

import org.junit.Test;

import com.sapphire.execution.AdapterWorker;

public class AdapterWorkerTest {

	@Test
	public void test() {
		AdapterWorker<Long> adapterWorker = new AdapterWorker<>("TestEngine", 2, 1_000_000, 100_000, 1_000);
		for (int i = 0; i < 10; i++) {
			adapterWorker.addAdapter(new TestAdapter());
		}
		long counter = 0;
		adapterWorker.getEngine().start();
		for (int i = 0; i < 10_000_000; i++) {
			counter++;
			try {
				adapterWorker.execute(counter);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
		adapterWorker.getEngine().stop();
		System.out.println(String.format("count:[%,d], enabled:[%,d], disabled:[%,d]", TestAdapter.COUNTER.get(), TestAdapter.ENABLED_COUNTER.get(), TestAdapter.DISABLED_COUNTER.get()));
	}

}
