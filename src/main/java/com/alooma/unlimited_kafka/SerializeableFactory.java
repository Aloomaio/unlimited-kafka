package com.alooma.unlimited_kafka;

public interface SerializeableFactory<T> {

    T fromBytes(byte[] rawData);
}
