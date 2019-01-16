package com.alooma.unlimitedKafka;

public interface SerializeableFactory<T> {

    T fromBytes(byte[] rawData);
}
