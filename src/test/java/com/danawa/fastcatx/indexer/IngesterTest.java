package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
import com.danawa.fastcatx.indexer.ingester.ProcedureIngester;
import com.github.fracpete.processoutput4j.output.ConsoleOutputProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.github.fracpete.rsync4j.Ssh;
import com.github.fracpete.rsync4j.core.Binaries;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.origin.SystemEnvironmentOrigin;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.WatchService;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IngesterTest {

    private static Logger logger = LoggerFactory.getLogger(IngesterTest.class);
    private String lastNdJson = "";

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
    public void testJDBCRead() throws IOException, SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://52.78.31.7:3306/new_schema?characterEncoding=utf-8";
        String user = "gncloud";
        String password = System.getProperty("password");
        String dataSQL = "SELECT * FROM food";
        ArrayList<String> sqlList = new ArrayList<String>();

        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        sqlList.add(dataSQL);

        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, bulkSize, fetchSize, maxRows, false, sqlList);
        while(ingester.hasNext()) {
            Map<String, Object> record = ingester.next();
            logger.info("{}", record);
        }
    }

    @Test
    public void testAlitibaseJDBCRead() throws IOException, SQLException {
        String driver = "Altibase.jdbc.driver.AltibaseDriver";
        String url = "jdbc:Altibase://112.175.252.198:20300/DNWALTI?ConnectionRetryCount=3&ConnectionRetryDelay=1&LoadBalance=off";
        String user = "DBLINKDATA_1";
        String password = "fldzmepdlxj**(";
        String dataSQL = "SELECT CATE_C FROM TCATE limit 1";
        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(dataSQL);

        JDBCIngester ingester = new JDBCIngester(driver, url, user, password, bulkSize, fetchSize, maxRows, false, sqlList);
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

    @Test
    public void testProcedureRead() throws IOException, InterruptedException {
        String driver = "Altibase.jdbc.driver.AltibaseDriver";
        String url = "jdbc:Altibase://112.175.252.198:20300/DNWALTI?ConnectionRetryCount=3&ConnectionRetryDelay=1&LoadBalance=off";
        String user = "DBLINKDATA_1";
        String password = "fldzmepdlxj**(";
        String procedureName = "search";
        String rsyncPah = "";
        Integer groupSeq = 0;

        String filePath = "D:\\result";

        int bulkSize = 1000;
        int fetchSize = 1000;
        int maxRows = 0;

        CallProcedure call = new CallProcedure(driver,url,user,password,procedureName,groupSeq,rsyncPah);
        call.callSearchProcedure();

        Thread.sleep(1000);

        ProcedureIngester ingester = new ProcedureIngester(filePath, "konan","CP949",1000,0);

        ReadFile th = new ReadFile(ingester);
        th.start();
    }

    class ReadFile extends Thread {

        private  ProcedureIngester ingester;
        ReadFile(ProcedureIngester ingester) {
            this.ingester = ingester;
        }

        public void run() {
            try{
                while(ingester.hasNext()) {
                        Map<String, Object> record = ingester.next();
                    logger.info("{}", record);
                 }
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testRsyncFile() throws Exception {
        RSync rsync = new RSync()
                .source("C:\\Users\\admin\\Desktop\\indexFile\\sample\\prodExt_5")
                .destination("D:\\result")
                .recursive(true)
                .progress(true)
                .inplace(true);

        ConsoleOutputProcessOutput output = new ConsoleOutputProcessOutput();
        output.monitor(rsync.builder());

    }

    //rsync 호출, rsync 파일을 바로 읽어 njson 형식으로..
    @Test
    public void readRsyncFile() throws Exception {

        Thread rsync = new RsyncFile();
        System.out.println("RSYNC");

        //rsync 쓰레드
        //rsync.start();
        //Thread.sleep(1000);


        FileLineWatcher watcher = new FileLineWatcher(new File("D:\\result\\prod5.ndjson"));

        Thread thread = new Thread(watcher);
        thread.setDaemon(true);

        thread.start();
        //파일 읽기 종료시까지..
        thread.join();

    }

    class RsyncFile  extends Thread {
        public void run() {

            RSync rsync = new RSync()
                    .source("C:\\Users\\admin\\Desktop\\indexFile\\sample\\prodExt_5")
                    .destination("D:\\result")
                    .recursive(true)
                    .archive(true)
                    .compress(true)
                    //.progress(true)
                    .bwlimit("20")
                    .inplace(true);

            ConsoleOutputProcessOutput output = new ConsoleOutputProcessOutput();
            try {
                output.monitor(rsync.builder());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class FileLineWatcher implements Runnable {
        private static final int DELAY_MILLIS = 1000;

        private boolean isRun;
        private final File file;

        public FileLineWatcher(File file) {
            this.file = file;

        }

        @Override
        public void run() {
            System.out.println("Start to tail a file - " + file.getName());

            isRun = true;
            if (!file.exists()) {
                System.out.println("not exists file - ");

            }

            //try 문에서 Stream을 열면 블럭이 끝났을 때 close를 해줌
            try (
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "CP949"))) {
                StringBuilder sb = new StringBuilder();

                int cnt = 0;
                while (isRun) {

                    if(cnt > 122346) {
                        stop();
                    }

                    String line = br.readLine();

                    if (line != null) {

                        //NDJSON
                        if(line.contains("{\"PRODUCTCODE\":") && line.contains("\"ADDDESCRIPTION\":")) {
                            cnt++;
                        }else{
                            System.out.println("sb append : " + line);
                            sb.append(line);
                        }

                        if(sb.toString().contains("{\"PRODUCTCODE\":") && sb.toString().contains("\"ADDDESCRIPTION\":")) {
                            System.out.println("sb " + cnt + " - " + (sb.toString()));
                            sb.setLength(0);
                        }else if(sb.length() > 14 && sb.toString().indexOf("{\"PRODUCTCODE\":") != 0) {
                            System.out.println("First Text is Not Product Code : " + sb.toString());
                            sb.setLength(0);
                        }

                        //KONAN
//                        if(line.contains("[%PRODUCTCODE%]") && line.contains("[%ADDDESCRIPTION%]")) {
//                            cnt ++;
//                        }else{
//                            sb.append(line);
//                           // System.out.println("sb append : " + line);
//                        }
//
//                        if(sb.toString().contains("[%PRODUCTCODE%]") && sb.toString().contains("[%ADDDESCRIPTION%]")){
//                            System.out.println("sb " + cnt + " - " + Utils.convertKonanToNdJson((sb.toString())));
//                            sb.setLength(0);
//                            cnt ++;
//                        }else if(sb.length() > 15 && sb.toString().indexOf("[%PRODUCTCODE%]") != 0){
//                            System.out.println("First Text is Not Product Code : " + sb.toString());
//                            sb.setLength(0);
//                        }

                    } else {
                        Thread.sleep(DELAY_MILLIS);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed to tail a file - " + file.getPath());
            }
            System.out.println("Stop to tail a file - " + file.getName());
        }

        public void stop() {
            isRun = false;
        }
    }

}
