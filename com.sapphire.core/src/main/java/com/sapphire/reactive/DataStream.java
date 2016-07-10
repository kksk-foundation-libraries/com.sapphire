package com.sapphire.reactive;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public class DataStream<DataType> implements AutoCloseable {
	private static final int DEFAULT_CAPACITY = 10_000;
	private boolean opened = false;

	private Observable<DataType> observable;
	private DataIterator<DataType> iterator;
	private Lock lock = new ReentrantLock();
	private Condition condition = lock.newCondition();
	private List<ListenerWraper<DataType>> list = new ArrayList<>();

	public void open() {
		openOn(DEFAULT_CAPACITY, null);
	}

	public void open(int capacity) {
		openOn(capacity, null);
	}

	public void openOn(Executor executor) {
		openOn(DEFAULT_CAPACITY, executor);
	}

	public synchronized void openOn(int capacity, Executor executor) {
		if (opened) {
			throw new RuntimeException("opened..");
		}
		iterator = new DataIterator<>(capacity);
		observable = Observable.create(new OnSubscribe<DataType>() {
			@Override
			public void call(Subscriber<? super DataType> t) {
				try {
					if (!t.isUnsubscribed()) {
						while (iterator.hasNext()) {
							t.onNext(iterator.next());
						}
						lock.lock();
						try {
							t.onCompleted();
							opened = false;
							condition.signalAll();
						} finally {
							lock.unlock();
						}
					}
				} catch (Exception e) {
					t.onError(e);
				}
			}
		});
		observable = observable.onBackpressureBuffer();
		if (executor != null) {
			observable = observable.subscribeOn(Schedulers.from(executor));
		} else {
			observable = observable.subscribeOn(Schedulers.io());
		}
		observable = observable.publish().refCount();
		opened = true;
	}

	public void write(DataType data) {
		iterator.put(data);
	}

	public <TranslatedDataType> DataStream<TranslatedDataType> map(Translator<DataType, TranslatedDataType> translator) {
		return map(translator, null);
	}

	public <TranslatedDataType> DataStream<TranslatedDataType> map(Translator<DataType, TranslatedDataType> translator, Executor executor) {
		DataStream<TranslatedDataType> dataStream = new DataStream<>();
		Observable<DataType> source = observable;
		if (executor != null) {
			source = source.observeOn(Schedulers.from(executor));
		}
		Observable<TranslatedDataType> dist = source.map(new Func1<DataType, TranslatedDataType>() {
			@Override
			public TranslatedDataType call(DataType t) {
				return translator.translate(t);
			}
		});
		dist = dist.onBackpressureBuffer();
		dist = dist.publish().refCount();
		dataStream.observable = dist;
		dataStream.opened = true;
		return dataStream;
	}

	public void listen(DataStreamListener<DataType> listener) {
		listenOn(listener, null);
	}

	public synchronized void listenOn(DataStreamListener<DataType> listener, Executor executor) {
		ListenerWraper<DataType> listenerWraper = new ListenerWraper<>(listener);
		list.add(listenerWraper);
		Observer<DataType> observer = listenerWraper;
		if (executor != null) {
			observable = observable.observeOn(Schedulers.from(executor));
		}
		observable.subscribe(observer);
	}

	private static class ListenerWraper<DataType> implements Observer<DataType> {
		private final DataStreamListener<DataType> listener;
		private final Lock lock = new ReentrantLock();
		private final Condition condition = lock.newCondition();
		private int status;

		public ListenerWraper(DataStreamListener<DataType> listener) {
			this.listener = listener;
			this.status = 0;
		}

		public void await() {
			lock.lock();
			try {
				if (status < 2) {
					condition.await();
				}
			} catch (InterruptedException e) {
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void onCompleted() {
			listener.onComplete();
			lock.lock();
			try {
				status = 2;
				condition.signalAll();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void onError(Throwable e) {
			listener.onError(e);
			lock.lock();
			try {
				status = 3;
				condition.signalAll();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void onNext(DataType t) {
			listener.onNext(t);
			lock.lock();
			try {
				status = 1;
			} finally {
				lock.unlock();
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (opened) {
			iterator.close();
			lock.lock();
			try {
				if (opened)
					condition.await();
				for (ListenerWraper<DataType> listenerWraper : list) {
					listenerWraper.await();
				}
			} finally {
				lock.unlock();
			}
		}
	}

	private static class DataIterator<DataType> implements Iterator<DataType>, AutoCloseable {
		private int readPosition = -1;
		private int writePosition = -1;

		private final Object[] circle;
		private final int circleSize;
		private static final Object POISON_PILL = new Object();
		private Lock lock = new ReentrantLock();
		private Condition condition = lock.newCondition();

		public DataIterator(int circleSize) {
			circle = new Object[circleSize];
			this.circleSize = circleSize;
		}

		@Override
		public boolean hasNext() {
			lock.lock();
			try {
				if (writePosition == readPosition)
					condition.await();
				return circle[getNextPosition(readPosition)] != POISON_PILL;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} finally {
				lock.unlock();
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public DataType next() {
			lock.lock();
			try {
				if (writePosition == readPosition)
					condition.await();
				int lastPosition = readPosition;
				readPosition = getNextPosition(readPosition);
				if (lastPosition >= 0)
					circle[lastPosition] = null;
				condition.signalAll();
				return (DataType) circle[readPosition];
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} finally {
				lock.unlock();
			}
		}

		public void put(DataType data) {
			put0(data);
		}

		private void put0(Object data) {
			lock.lock();
			try {
				if (readPosition == getNextPosition(writePosition))
					condition.await();
				writePosition = getNextPosition(writePosition);
				circle[writePosition] = data;
				condition.signalAll();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			} finally {
				lock.unlock();
			}
		}

		private int getNextPosition(int position) {
			if (position + 1 == circleSize) {
				return 0;
			} else {
				return position + 1;
			}
		}

		@Override
		public void close() throws Exception {
			put0(POISON_PILL);
		}
	}
}
