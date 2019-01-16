package com.alooma.unlimited_kafka.exceptions;

public class UnpackException extends RuntimeException {

    public UnpackException(Exception ex) {
        super(ex);
    }
}
