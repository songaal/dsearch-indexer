package com.danawa.fastcatx.indexer;

import java.util.Map;

public interface Filter {
    default void init(Map<String, Object> payload) {}
    Map<String, Object> filter(Map<String, Object> item);
}
