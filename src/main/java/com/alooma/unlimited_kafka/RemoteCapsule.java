package com.alooma.unlimited_kafka;

public class RemoteCapsule<T> implements Capsule<T> {
    private String key;

    public RemoteCapsule(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
