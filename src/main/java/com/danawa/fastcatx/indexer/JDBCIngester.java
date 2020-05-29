package com.danawa.fastcatx.indexer;

import java.io.IOException;
import java.util.Map;

public class JDBCIngester implements Ingester {
    public JDBCIngester() {

    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public Map<String, Object> next() throws IOException {
        return null;
    }
}
