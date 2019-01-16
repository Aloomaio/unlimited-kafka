package com.alooma.unlimitedKafka.unpacker;

import com.alooma.unlimitedKafka.Capsule;

public interface MessageUnpacker<T> {
    T unpackMessage(Capsule<T> capsule);
}
