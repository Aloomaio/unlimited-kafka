package com.alooma.unlimited_kafka;

public class LocalCapsule<T> implements Capsule<T> {
    private T data;

    public LocalCapsule(T data) {
        this.data = data;
    }

    public T getData() {
        return data;
    }
}
