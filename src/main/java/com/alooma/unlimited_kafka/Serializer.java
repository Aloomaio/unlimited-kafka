package com.alooma.unlimited_kafka;


public interface Serializer<T> {

    byte[] serialize(T data);
}
