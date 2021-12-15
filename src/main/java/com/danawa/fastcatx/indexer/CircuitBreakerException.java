package com.danawa.fastcatx.indexer;

public class CircuitBreakerException extends Exception {
    CircuitBreakerException(String msg){
        super(msg);
    }
}
