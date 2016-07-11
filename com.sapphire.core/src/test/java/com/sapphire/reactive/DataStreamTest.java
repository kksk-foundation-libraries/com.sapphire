package com.sapphire.reactive;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import com.sapphire.metrics.StopWatch;
import com.sapphire.reactive.DataStream;
import com.sapphire.reactive.DataStreamListener;

public class DataStreamTest {
	private static final boolean SKIP1 = true;
	private static final boolean SKIP2 = true;
	private static final boolean SKIP3 = true;
	private static final boolean SKIP4 = true;
	private static final boolean SKIP5 = true;
	private static final boolean SKIP6 = true;
	private static final boolean SKIP7 = false;
	private static final int QUEUE_SIZE = 100_000;
	private static final int TEST_COUNT = 10_000_000;
	private static final StopWatch STOP_WATCH = new StopWatch();

	private static final ThreadPoolExecutor executor1 = new ThreadPoolExecutor(2, 2, 1, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100_000));
	private static final ThreadPoolExecutor executor2 = new ThreadPoolExecutor(2, 2, 1, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(100_000));

	private static class DataType1 {
	}

	private static class DataType2 {
	}

	private static class DataStreamListener1 implements DataStreamListener<DataType1> {
		static final AtomicLong count = new AtomicLong();
		static final AtomicLong error = new AtomicLong();

		static void reset() {
			count.set(0);
			error.set(0);
		}

		@Override
		public void onNext(DataType1 data) {
			count.getAndIncrement();
		}

		@Override
		public void onError(Throwable e) {
			e.printStackTrace();
			error.getAndIncrement();
		}

		@Override
		public void onComplete() {
			System.out.println(String.format("listener1:{count:[%,d], error:[%,d]}", count.get(), error.get()));
		}
	}

	private static class DataStreamListener2 implements DataStreamListener<DataType2> {
		static final AtomicLong count = new AtomicLong();
		static final AtomicLong error = new AtomicLong();

		static void reset() {
			count.set(0);
			error.set(0);
		}

		@Override
		public void onNext(DataType2 data) {
			count.getAndIncrement();
		}

		@Override
		public void onError(Throwable e) {
			e.printStackTrace();
			error.getAndIncrement();
		}

		@Override
		public void onComplete() {
			System.out.println(String.format("listener2:{count:[%,d], error:[%,d]}", count.get(), error.get()));
		}
	}

	private static class DataStreamListener3 implements DataStreamListener<DataType1> {
		static final AtomicLong count = new AtomicLong();
		static final AtomicLong error = new AtomicLong();

		static void reset() {
			count.set(0);
			error.set(0);
		}

		@Override
		public void onNext(DataType1 data) {
			count.getAndIncrement();
		}

		@Override
		public void onError(Throwable e) {
			e.printStackTrace();
			error.getAndIncrement();
		}

		@Override
		public void onComplete() {
			System.out.println(String.format("listener3:{count:[%,d], error:[%,d]}", count.get(), error.get()));
		}
	}

	@Test
	public void test000() {
		test001();
		test002();
		test003();
		test004();
		test005();
		test006();
		test007();
	}

	// @Test
	public void test001() {
		if (SKIP1)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.open(QUEUE_SIZE);
		dataStream1.listen(new DataStreamListener1());
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test002() {
		if (SKIP2)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.openOn(QUEUE_SIZE, executor1);
		dataStream1.listen(new DataStreamListener1());
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test003() {
		if (SKIP3)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.open(QUEUE_SIZE);
		dataStream1.listenOn(new DataStreamListener1(), executor2);
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test004() {
		if (SKIP4)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.openOn(QUEUE_SIZE, executor1);
		dataStream1.listenOn(new DataStreamListener1(), executor2);
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test005() {
		if (SKIP5)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.open(QUEUE_SIZE);
		dataStream1.listen(new DataStreamListener1());
		final DataType2 dataType2 = new DataType2();
		DataStream<DataType2> dataStream2 = dataStream1.map(new Translator<DataType1, DataType2>() {
			@Override
			public DataType2 translate(DataType1 input) {
				return dataType2;
			}
		});
		dataStream2.listen(new DataStreamListener2());
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test006() {
		if (SKIP6)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.openOn(QUEUE_SIZE, executor1);
		dataStream1.listen(new DataStreamListener1());
		final DataType2 dataType2 = new DataType2();
		DataStream<DataType2> dataStream2 = dataStream1.map(new Translator<DataType1, DataType2>() {
			@Override
			public DataType2 translate(DataType1 input) {
				return dataType2;
			}
		});
		dataStream2.listenOn(new DataStreamListener2(), executor2);
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

	// @Test
	public void test007() {
		if (SKIP7)
			return;
		DataStreamListener1.reset();
		DataStreamListener2.reset();
		DataStreamListener3.reset();

		final DataType1 dataType1 = new DataType1();
		DataStream<DataType1> dataStream1 = new DataStream<>();
		dataStream1.openOn(QUEUE_SIZE, executor1);
		dataStream1.listen(new DataStreamListener1());
		dataStream1.listen(new DataStreamListener3());
		STOP_WATCH.start();
		for (long i = 0; i < TEST_COUNT; i++) {
			dataStream1.write(dataType1);
		}
		try {
			dataStream1.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			STOP_WATCH.stop();
		}
	}

}
