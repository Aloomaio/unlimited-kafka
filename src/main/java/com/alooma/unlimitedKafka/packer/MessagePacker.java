package com.alooma.unlimitedKafka.packer;

import com.alooma.unlimitedKafka.Capsule;

public interface MessagePacker<T> {
    Capsule<T> packMessage(T message, String topic, Long offset);
}
