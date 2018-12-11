package com.alooma.unlimited_kafka.unpacker;

import com.alooma.unlimited_kafka.Capsule;

public interface MessageUnpacker<T> {
    T unpackMessage(Capsule<T> capsule);
}
