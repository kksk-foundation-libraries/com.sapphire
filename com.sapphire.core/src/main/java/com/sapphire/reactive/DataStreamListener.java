package com.sapphire.reactive;

public interface DataStreamListener<DataType> {
	void onNext(DataType data);

	void onError(Throwable e);

	void onComplete();
}
