package com.danawa.fastcatx.indexer.preProcess;

import Altibase.jdbc.driver.AltibasePreparedStatement;
import com.danawa.fastcatx.indexer.Utils;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class VmFirstMakeDatePreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(VmFirstMakeDatePreProcess.class);
    private Job job;
    public VmFirstMakeDatePreProcess(Job job) {
        this.job = job;

    }

    public void start() throws Exception {
        Map<String, Object> payload = job.getRequest();
        String dataSQL = (String) payload.get("dataSQL");
        String env = (String) payload.get("env");
        String alti_master_url = (String) payload.get("alti_master_url");
        String alti_slave_url = (String) payload.getOrDefault("alti_slave_url",null);
        String alti_rescue_url = (String) payload.getOrDefault("alti_rescue_url",null);
        String alti_user = (String) payload.get("alti_user");
        String alti_password = (String) payload.get("alti_password");


        Connection selectConn = null;
        Connection insertConn = null;
        AltibasePreparedStatement insertPstmt = null;

        // alti_slave_url = jdbc:Altibase://alti1-slave1.danawa.com:20300/DNWALTI?AlternateServers=(alti1-rescue.danawa.com:20300)&SessionFailOver=off&ConnectionRetryCount=2&ConnectionRetryDelay=0&LoadBalance=off
        // alti_user = DBLINKDATA_A
        // alti_password = ektbfm#^^
        try {
            // 1. select
            selectConn = DriverManager.getConnection(alti_slave_url, alti_user, alti_password);
            long selectStart = System.currentTimeMillis(); // SELETE TIME 시작
            ResultSet selectResult = selectConn.prepareStatement(dataSQL).executeQuery(); // 쿼리문 전송
            long selectEnd = System.currentTimeMillis(); // SELETE TIME 끝
            logger.info("SELECT {}", Utils.calcSpendTime(selectStart, selectEnd));

            if (selectResult == null) {
                logger.error("vmFirstMakeDate error - sql select result null!");
            }

            // 2. truncate
            DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();
            Boolean isEmpty = true;
//            Boolean isEmpty = truncateManager.linkTruncateCall(env, "tFirstDateForSearch", alti_master_url, alti_slave_url, alti_rescue_url, alti_user, alti_password);
            if(!databaseQueryHelper.truncate(selectConn, "tFirstDateForSearch")) {
                isEmpty = false;
            }
            //....

            // 3. insert
            long insertStart = System.currentTimeMillis(); // INSERT TIME 시작
            String insertQuerys = String.format("INSERT INTO TFIRSTDATEFORSEARCH (PROD_C , FIRSTDATE) VALUES (?,?)");
            insertPstmt = (AltibasePreparedStatement) insertConn.prepareStatement(insertQuerys);
            insertPstmt.setAtomicBatch(true);
            int totalCount = 0;

            // truncate success -> insert
            if (isEmpty) {
                while (selectResult.next()) {
                    insertPstmt.setInt(1, selectResult.getInt("PROD_C"));
                    insertPstmt.setDate(2, selectResult.getDate("FIRSTDATE"));
                    insertPstmt.addBatch();
                    totalCount++;
                    if (totalCount % 500 == 0) {
                        insertPstmt.executeBatch();
                        insertPstmt.clearBatch();
                    }
                }
                if (totalCount % 500 != 0) {
                    insertPstmt.executeBatch();
                    insertPstmt.clearBatch();
                }
                long insertEnd = System.currentTimeMillis(); // INSERT TIME 끝
                logger.info("INSERT {}", Utils.calcSpendTime(insertStart, insertEnd));
                logger.info("vmFirstMakeDate count : {}", totalCount);

            }
        } catch (SQLException e) {
            logger.error("vmFirstMakeDate fail", e.getMessage());
        } finally {
            try {
                selectConn.close();
                insertPstmt.close();
                insertConn.close();
            } catch (SQLException e) {
                logger.error("connection close fail", e.getMessage());
            }
        }

    }

}
