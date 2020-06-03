package com.danawa.fastcatx.indexer.filter;

import com.danawa.fastcatx.indexer.Filter;

import java.util.Map;

public class MockFilter implements Filter {
    @Override
    public Map<String, Object> filter(Map<String, Object> item) {
        // do nothing..
        return item;
    }
}
