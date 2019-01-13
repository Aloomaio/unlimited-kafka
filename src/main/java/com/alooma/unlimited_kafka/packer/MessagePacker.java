package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;

import java.io.IOException;

public interface MessagePacker<T> {
    Capsule<T> packMessage(T message, String topic, Long offset) throws InterruptedException, IOException;
}
