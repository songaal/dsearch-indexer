package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.impl.NDJsonIngester;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class NDJsonIngesterTest {

    private static Logger logger = LoggerFactory.getLogger(NDJsonIngesterTest.class);

    @Test
    public void testRead() throws IOException {
        String filePath = "C:\\Projects\\fastcatx-indexer\\src\\test\\resources\\sample.ndjson";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }
}
