package com.alooma.unlimited_kafka;


import java.util.Objects;

public class Capsule<T> {

    private final Type type;
    private final String key;
    private final T data;

    private Capsule(Type type, String key, T data) {
        this.type = type;
        this.key = key;
        this.data = data;
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public T getData() {
        return data;
    }

    public enum Type {
        LOCAL,
        REMOTE
    }

    public static <T> Capsule<T> localCapsule(T data) {
        return new Capsule<>(Type.LOCAL, null, data);
    }

    public static <T> Capsule<T> remoteCapsule(String key) {
        return new Capsule<>(Type.REMOTE, key, null);
    }

    @Override
    public String toString() {
        return "Capsule{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", data=" + data +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Capsule)) return false;
        Capsule<?> capsule = (Capsule<?>) o;
        return type == capsule.type &&
                Objects.equals(key, capsule.key) &&
                Objects.equals(data, capsule.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, key, data);
    }
}
