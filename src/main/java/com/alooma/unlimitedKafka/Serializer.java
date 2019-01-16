package com.alooma.unlimitedKafka;


public interface Serializer<T> {

    byte[] serialize(T data);
}

