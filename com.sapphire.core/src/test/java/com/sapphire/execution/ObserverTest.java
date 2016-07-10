package com.sapphire.execution;

import org.junit.Test;

import rx.Observable;
import rx.Observer;
import rx.schedulers.Schedulers;

public class ObserverTest {
	private static final boolean SKIP_001 = false;
	private static final boolean SKIP_002 = false;
	private static final boolean SKIP_003 = true;

	final Observable<Long> o = Observable.create(onSubscribe -> {
		try {
			for (long i = 0; i < 1_000_000_000; i++) {
				if (onSubscribe.isUnsubscribed()) {
					return;
				}
				onSubscribe.onNext(i);
			}
			if (!onSubscribe.isUnsubscribed()) {
				onSubscribe.onCompleted();
			}
		} catch (Throwable e) {
			if (!onSubscribe.isUnsubscribed()) {
				onSubscribe.onError(e);
			}
		}
	});

	@Test
	public void test001() {
		if (SKIP_001)
			return;
		// async
		o.observeOn(Schedulers.immediate()).subscribe(new LongObserver(System.nanoTime()));
		System.out.println("Called...");
	}

	@Test
	public void test002() {
		if (SKIP_002)
			return;
		// sync
		o.subscribe(new LongObserver(System.nanoTime()));
		System.out.println("Called...");
	}

	private static class LongObserver implements Observer<Long> {
		final long start;
		long last = -1;
		long xxx = 0;

		public LongObserver(long start) {
			this.start = start;
		}

		@Override
		public void onNext(Long t) {
			if (last < t.longValue()) {
				last = t.longValue();
			} else {
				xxx++;
			}
		}

		@Override
		public void onError(Throwable e) {
			System.out.println(e);
		}

		@Override
		public void onCompleted() {
			System.out.println(String.format("completed:[%,d], last:[%,d], xxx:[%,d]", System.nanoTime() - start, last, xxx));
		}
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
