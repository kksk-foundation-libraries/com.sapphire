package com.sapphire.execution;

import org.junit.Test;

import com.sapphire.execution.AdapterWorker;

public class AdapterWorkerTest {
	private static final boolean SKIP_001 = true;
	private static final boolean SKIP_002 = true;
	private static final boolean SKIP_003 = false;

	@Test
	public void test001() {
		if (SKIP_001)
			return;
		// async
		TestAdapter.COUNTER.set(0);
		TestAdapter.ENABLED_COUNTER.set(0);
		TestAdapter.DISABLED_COUNTER.set(0);
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

	@Test
	public void test002() {
		if (SKIP_002)
			return;
		// sync
		TestAdapter.COUNTER.set(0);
		TestAdapter.ENABLED_COUNTER.set(0);
		TestAdapter.DISABLED_COUNTER.set(0);
		AdapterWorker<Long> adapterWorker = new AdapterWorker<>("TestEngine", 10, 1_000_000, 100_000, 1_000, true);
		for (int i = 0; i < 100; i++) {
			adapterWorker.addAdapter(new TestAdapter());
		}
		long counter = 0;
		adapterWorker.getEngine().start();
		for (int i = 0; i < 1_000_000; i++) {
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

	@Test
	public void test003() {
		if (SKIP_003)
			return;
		// async-sync
		TestAdapter.COUNTER.set(0);
		TestAdapter.ENABLED_COUNTER.set(0);
		TestAdapter.DISABLED_COUNTER.set(0);
		Engine engine1 = new Engine("TestEngine1", 10, 1_000_000);
		Engine engine2 = new Engine("TestEngine2", 20, 1_000_000);
		AdapterWorker<Long> adapterWorkerAsync = new AdapterWorker<>(engine1, 1_000_000, 100_000, 1_000);
		final AdapterWorker<Long> adapterWorkerSync = new AdapterWorker<>(engine2, 1_000_000, 100_000, 1_000, true);
		Adapter<Long> adapter = new Adapter<Long>() {
			@Override
			public void execute(Long message) {
				try {
					adapterWorkerSync.execute(message);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public boolean enabled() {
				return true;
			}

			@Override
			public boolean checkEnabled() {
				return true;
			}
		};
		for (int i = 0; i < 100; i++) {
			adapterWorkerAsync.addAdapter(adapter);
		}
		for (int i = 0; i < 100; i++) {
			adapterWorkerSync.addAdapter(new TestAdapter());
		}
		long counter = 0;
		engine1.start();
		engine2.start();
		long start = System.nanoTime();
		for (int i = 0; i < 1_000_000; i++) {
			counter++;
			try {
				adapterWorkerAsync.execute(counter);
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
		engine1.stop();
		engine2.stop();
		long stop = System.nanoTime();
		System.out.println(String.format("count:[%,d], enabled:[%,d], disabled:[%,d], elapsed:[%,.06f]", TestAdapter.COUNTER.get(), TestAdapter.ENABLED_COUNTER.get(), TestAdapter.DISABLED_COUNTER.get(), (stop - start) / 1000000d));
	}

}
