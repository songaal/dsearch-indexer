package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.impl.CSVIngester;
import com.danawa.fastcatx.indexer.impl.JDBCIngester;
import com.danawa.fastcatx.indexer.impl.NDJsonIngester;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class IngesterTest {

    private static Logger logger = LoggerFactory.getLogger(IngesterTest.class);

    @Test
    public void testJsonRead() throws IOException {
        String filePath = "sample/sample.ndjson";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }

    @Test
    public void testCSVRead() throws IOException {
        String filePath = "sample/food.csv";
        logger.info("path: {}" ,new File(filePath).getAbsolutePath());
        CSVIngester ingester = new CSVIngester(filePath, "utf-8", 1000);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }

    @Test
    public void testJDBCRead() throws IOException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://52.78.31.7:3306/new_schema?characterEncoding=utf-8";
        String user = "gncloud";
        String password = "gnc=1153";//System.getProperty("password");
        String dataSQL = "SELECT * FROM food";
        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, dataSQL, bulkSize, fetchSize, maxRows, false);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }
}
