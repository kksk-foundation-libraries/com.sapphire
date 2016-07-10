package com.sapphire.reactive;

public interface Translator<BeforeDataType, AfterDataType> {
	AfterDataType translate(BeforeDataType input);
}
