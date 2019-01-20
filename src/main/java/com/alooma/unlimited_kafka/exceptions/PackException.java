package com.alooma.unlimited_kafka.exceptions;

public class PackException extends RuntimeException {

    public PackException(Exception ex) {
        super(ex);
    }
}
