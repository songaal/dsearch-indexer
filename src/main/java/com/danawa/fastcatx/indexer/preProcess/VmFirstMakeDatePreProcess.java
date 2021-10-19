package com.danawa.fastcatx.indexer.preProcess;

import Altibase.jdbc.driver.AltibaseConnection;
import Altibase.jdbc.driver.AltibasePreparedStatement;
import com.danawa.fastcatx.indexer.IndexJobRunner;
import com.danawa.fastcatx.indexer.Utils;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.Map;

public class VmFirstMakeDatePreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(VmFirstMakeDatePreProcess.class);
    private Job job;
    private Map<String, Object> payload;
    private enum DB_TYPE {master, slave, rescue}

    public VmFirstMakeDatePreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }
    public void start() throws Exception {
        logger.info("최조제조일 전처리를 시작합니다.");
        // 필수 파라미터
        String selectSql = (String) payload.getOrDefault("selectSql", "");
        String insertSql = (String) payload.getOrDefault("insertSql", "");
        String tableName = (String) payload.getOrDefault("tableName", ""); // tFirstDateForSearch

        String altibaseDriver = (String) payload.getOrDefault("altibaseDriver", "");
        String altibaseMasterAddress = (String) payload.getOrDefault("altibaseMasterAddress", "");
        String altibaseMasterUsername = (String) payload.getOrDefault("altibaseMasterUsername", "");
        String altibaseMasterPassword = (String) payload.getOrDefault("altibaseMasterPassword", "");
        boolean altibaseSlaveEnable = (boolean) payload.getOrDefault("altibaseSlaveEnable", false);
        String altibaseSlaveAddress = (String) payload.getOrDefault("altibaseSlaveAddress", "");
        String altibaseSlaveUsername = (String) payload.getOrDefault("altibaseSlaveUsername", "");
        String altibaseSlavePassword = (String) payload.getOrDefault("altibaseSlavePassword", "");
        boolean altibaseRescueEnable = (boolean) payload.getOrDefault("altibaseRescueEnable", false);
        String altibaseRescueAddress = (String) payload.getOrDefault("altibaseRescueAddress", "");
        String altibaseRescueUsername = (String) payload.getOrDefault("altibaseRescueUsername", "");
        String altibaseRescuePassword = (String) payload.getOrDefault("altibaseRescuePassword", "");

        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn(VmFirstMakeDatePreProcess.DB_TYPE.master.name(), altibaseDriver, altibaseMasterAddress, altibaseMasterUsername, altibaseMasterPassword);
        if (altibaseSlaveEnable) {
            databaseConnector.addConn(VmFirstMakeDatePreProcess.DB_TYPE.slave.name(), altibaseDriver, altibaseSlaveAddress, altibaseSlaveUsername, altibaseSlavePassword);
        }
        if (altibaseRescueEnable) {
            databaseConnector.addConn(VmFirstMakeDatePreProcess.DB_TYPE.rescue.name(), altibaseDriver, altibaseRescueAddress, altibaseRescueUsername, altibaseRescuePassword);
        }

        try (AltibaseConnection masterConnection = databaseConnector.getConnAlti(VmFirstMakeDatePreProcess.DB_TYPE.master.name());
             AltibaseConnection selectSlaveConnection = databaseConnector.getConnAlti(VmFirstMakeDatePreProcess.DB_TYPE.slave.name());
             AltibaseConnection slaveConnection = databaseConnector.getConnAlti(VmFirstMakeDatePreProcess.DB_TYPE.slave.name());
             AltibaseConnection rescueConnection = databaseConnector.getConnAlti(VmFirstMakeDatePreProcess.DB_TYPE.rescue.name());
        )
        {
            // Connection이 정상적으로 이루어지지 않음.
            if(masterConnection == null
                    || (altibaseSlaveEnable && selectSlaveConnection == null)
                    || (altibaseSlaveEnable && slaveConnection == null)
                    || (altibaseRescueEnable && rescueConnection == null)){
                logger.error("masterConnection: {}, selectSlaveConnection: {}, slaveConnection: {}, rescueConnection: {}",
                        masterConnection, selectSlaveConnection, slaveConnection, rescueConnection);
                // 따라서 Error status 부여
                job.setStatus(IndexJobRunner.STATUS.ERROR.name());
                return;
            }

            DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();
            // 1. select
            long selectStart = System.currentTimeMillis(); // SELETE TIME 시작
            ResultSet resultSet = databaseQueryHelper.simpleSelectForwadOnly(altibaseSlaveEnable ? selectSlaveConnection : masterConnection, selectSql);
            int rowCount = resultSet.getRow();
            logger.info("조회 Row 갯수: {}, SQL: {}", rowCount, selectSql.substring(0, 100));
            long selectEnd = System.currentTimeMillis(); // SELETE TIME 끝
            logger.info("SELECT {}", Utils.calcSpendTime(selectStart, selectEnd));

            if (resultSet == null) {
                logger.error("VmFirstMakeDatePreProcess error - sql select result null!");
                throw new SQLException("sql select result null");
            }

            // 2. truncate
            // slave, rescue 선택적으로 truncate 호출
            boolean isSlaveTruncated = false;
            boolean isRescueTruncated = false;
            if (altibaseRescueEnable) {
                Thread.sleep(5000);
                isRescueTruncated = databaseQueryHelper.truncate(rescueConnection, tableName);
                logger.info("[rescue] truncate result: {}", isRescueTruncated);
            } else {
                isRescueTruncated = true;
            }
            if (altibaseSlaveEnable) {
                Thread.sleep(5000);
                isSlaveTruncated = databaseQueryHelper.truncate(slaveConnection, tableName);
                logger.info("[slave] truncate result: {}", isSlaveTruncated);
            } else {
                isSlaveTruncated = true;
            }
            boolean isMasterTruncated = databaseQueryHelper.truncate(masterConnection, tableName);
            logger.info("[master] truncate result: {}", isMasterTruncated);
            if (!isMasterTruncated || !isSlaveTruncated || !isRescueTruncated) {
                logger.warn("Truncate 실패했습니다.");
                throw new SQLException("Truncate failure");
            }

            // 3. insert
            long insertStart = System.currentTimeMillis(); // INSERT TIME 시작
            PreparedStatement preparedStatement = masterConnection.prepareStatement(insertSql);
            AltibasePreparedStatement insertPstmt =  (AltibasePreparedStatement) preparedStatement;
            insertPstmt.setAtomicBatch(true);
            int totalCount = 0;
            // truncate success -> insert
            while (resultSet.next()) {
                insertPstmt.setInt(1, resultSet.getInt("PROD_C"));
                insertPstmt.setDate(2, resultSet.getDate("FIRSTDATE"));
                insertPstmt.addBatch();
                totalCount++;
                if (totalCount % 10000 == 0) {
                    insertPstmt.executeBatch();
                    insertPstmt.clearBatch();
                    logger.info("데이터를 추가하였습니다. {} / {}", totalCount, rowCount);
                }
            }
            if (totalCount % 10000 != 0) {
                insertPstmt.executeBatch();
                insertPstmt.clearBatch();
            }
            logger.info("데이터를 추가하였습니다. {} / {}", totalCount, rowCount);
            long insertEnd = System.currentTimeMillis(); // INSERT TIME 끝
            logger.info("INSERT {}", Utils.calcSpendTime(insertStart, insertEnd));
            logger.info("최초제조일 갱신 완료! count : {}", totalCount);
        }

        job.setStatus(IndexJobRunner.STATUS.SUCCESS.name());
    }
}