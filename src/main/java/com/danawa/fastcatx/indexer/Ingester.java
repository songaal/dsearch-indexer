package com.danawa.fastcatx.indexer;

import java.io.IOException;
import java.util.Map;

public interface Ingester {

    public boolean hasNext() throws IOException;

    public Map<String, Object> next() throws IOException;
}
