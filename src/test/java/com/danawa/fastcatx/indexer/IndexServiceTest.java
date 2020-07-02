package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class IndexServiceTest {

    private static Logger logger = LoggerFactory.getLogger(IngesterTest.class);

    String host = "es1.danawa.io";
    Integer port = 80;
    String scheme = "http";
    String index = "prodext-s";
    Integer bulkSize = 1000;

    @Test
    public void testJson2Search() throws IOException {
        String filePath = "C:\\Users\\admin\\data\\converted\\prodExt_6_all_utf8";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        if (indexService.existsIndex(index)) {
            indexService.deleteIndex(index);
        }
        indexService.index(ingester, index, bulkSize, null);
    }

    @Test
    public void testJson2SearchMultiThreads() throws IOException {
        int threadSize = 20;
        String filePath = "C:\\Users\\admin\\data\\converted\\prodExt_6_all_utf8";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        if (indexService.existsIndex(index)) {
            indexService.deleteIndex(index);
        }
        indexService.indexParallel(ingester, index, bulkSize, null, threadSize);
    }

    @Test
    public void testCVS2Search() throws IOException {
        String filePath = "sample/food.csv";
        logger.info("path: {}", new File(filePath).getAbsolutePath());
        CSVIngester ingester = new CSVIngester(filePath, "utf-8", 1000);
        Integer bulkSize = 1000;
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.index(ingester, index, bulkSize, null);
    }

    @Test
    public void testJDBC2Search() throws IOException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://52.78.31.7:3306/new_schema?characterEncoding=utf-8";
        String user = "gncloud";
        String password = System.getProperty("password");
        String dataSQL = "SELECT * FROM food";
        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;
        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, dataSQL, bulkSize, fetchSize, maxRows, false);
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.index(ingester, index, bulkSize, null);
    }

    @Test
    public void testFastCatDI() throws IOException {

        host = "stest3.danawa.com";
        port = 8090;
        scheme = "http";
        index = "TEST_V1,TEST_V2";
        Filter filter = (Filter) Utils.newInstance("com.danawa.fastcatx.indexer.filter.DanawaProductFilter");

        String filePath = "C:\\Users\\admin\\Desktop\\indexFile\\test.ndjson";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.fastcatDynamicIndex(ingester, index, filter);
    }

    @Test
    public void testESDI() throws IOException {

        index = "prod1,prod2";
        Filter filter = (Filter) Utils.newInstance("com.danawa.fastcatx.indexer.filter.DanawaProductFilter");

        String filePath = "C:\\Users\\admin\\Desktop\\indexFile\\test.ndjson";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.elasticDynamicIndex(ingester, index, filter);
    }

    public void testStorageSize() {
        IndexService indexService = new IndexService(host, port, scheme);
//        indexService. getStorageSize()
    }
}
