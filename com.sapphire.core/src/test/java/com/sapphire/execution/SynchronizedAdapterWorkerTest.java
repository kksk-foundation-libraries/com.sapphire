package com.sapphire.execution;

import org.junit.Test;

public class SynchronizedAdapterWorkerTest {
	@Test
	public void test() {
		SynchronizedAdapterWorker<LongMessage> adapterWorker = new SynchronizedAdapterWorker<>("TestEngine", 4, 1_000_000, 100_000, 1_000);
		for (int i = 0; i < 100; i++) {
			adapterWorker.addAdapter(new TestAdapter());
		}
		long counter = 0;
		adapterWorker.getEngine().start();
		for (int i = 0; i < 1_000_000; i++) {
			counter++;
			try {
				LongMessage message = new LongMessage(counter);
				adapterWorker.execute(message);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
		adapterWorker.getEngine().stop();
		System.out.println(String.format("count:[%,d], enabled:[%,d], disabled:[%,d]", TestAdapter.COUNTER.get(), TestAdapter.ENABLED_COUNTER.get(), TestAdapter.DISABLED_COUNTER.get()));
	}

}
