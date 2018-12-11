package com.alooma.unlimited_kafka.packer;

import com.alooma.unlimited_kafka.Capsule;

interface MessagePacker<T> {
    Capsule<T> packMessage(T message, String topic, Long offset);
}
