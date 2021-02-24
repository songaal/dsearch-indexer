package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

public class IndexServiceTest {

    private static Logger logger = LoggerFactory.getLogger(IngesterTest.class);

    String host = "es1.danawa.io";
    Integer port = 80;
    String scheme = "http";
    String index = "prodext-s";
    Integer bulkSize = 1000;

    @Test
    public void testJson2Search() throws IOException, StopSignalException {
        String filePath = "C:\\Users\\admin\\data\\converted\\prodExt_6_all_utf8";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        if (indexService.existsIndex(index)) {
            indexService.deleteIndex(index);
        }
        indexService.index(ingester, index, bulkSize, null, null);
    }

    @Test
    public void testJson2SearchMultiThreads() throws IOException, StopSignalException {
        int threadSize = 20;
        String filePath = "C:\\Users\\admin\\data\\converted\\prodExt_6_all_utf8";
        NDJsonIngester ingester = new NDJsonIngester(filePath, "utf-8", 1000);
        IndexService indexService = new IndexService(host, port, scheme);
        if (indexService.existsIndex(index)) {
            indexService.deleteIndex(index);
        }
        indexService.indexParallel(ingester, index, bulkSize, null, threadSize, null);
    }

    @Test
    public void testCVS2Search() throws IOException, StopSignalException {
        String filePath = "sample/food.csv";
        logger.info("path: {}", new File(filePath).getAbsolutePath());
        CSVIngester ingester = new CSVIngester(filePath, "utf-8", 1000);
        Integer bulkSize = 1000;
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.index(ingester, index, bulkSize, null, null);
    }

    @Test
    public void testJDBC2Search() throws IOException, StopSignalException, SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://52.78.31.7:3306/new_schema?characterEncoding=utf-8";
        String user = "gncloud";
        String password = System.getProperty("password");
        String dataSQL = "SELECT * FROM food";
        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(dataSQL);

        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, bulkSize, fetchSize, maxRows, false, sqlList);
        IndexService indexService = new IndexService(host, port, scheme);
        indexService.index(ingester, index, bulkSize, null, null);
    }

    public void testStorageSize() {
        IndexService indexService = new IndexService(host, port, scheme);
//        indexService. getStorageSize()
    }
}
