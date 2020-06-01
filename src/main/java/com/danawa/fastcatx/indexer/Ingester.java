package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public interface Ingester {
    Logger logger = LoggerFactory.getLogger(Ingester.class);

    boolean hasNext() throws IOException;

    Map<String, Object> next() throws IOException;

    void close() throws IOException;
}
