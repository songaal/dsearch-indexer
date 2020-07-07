package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
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
        String password = System.getProperty("password");
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

    @Test
    public void testAlitibaseJDBCRead() throws IOException {
        String driver = "Altibase.jdbc.driver.AltibaseDriver";
        String url = "jdbc:Altibase://112.175.252.198:20300/DNWALTI?ConnectionRetryCount=3&ConnectionRetryDelay=1&LoadBalance=off";
        String user = "DBLINKDATA_1";
        String password = "fldzmepdlxj**(";
        String dataSQL = "SELECT CATE_C FROM TCATE limit 1";
        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, dataSQL, bulkSize, fetchSize, maxRows, false);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }

    @Test
    public void testJsonReadAndFilter() throws IOException {
        String filePath = "sample/sample.ndjson";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        String filterClassName = "com.danawa.fastcatx.indexer.filter.MockFilter";
        Filter filter = (Filter) Utils.newInstance(filterClassName);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            if (filter != null) {
                record = filter.filter(record);
            }
            logger.info("{}", record);
        }
    }
}
