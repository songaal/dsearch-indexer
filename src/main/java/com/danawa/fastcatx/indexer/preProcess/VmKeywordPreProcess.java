package com.danawa.fastcatx.indexer.preProcess;

import Altibase.jdbc.driver.AltibasePreparedStatement;
import com.danawa.fastcatx.indexer.Utils;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class VmKeywordPreProcess {
    private static final Logger logger = LoggerFactory.getLogger(VmFirstMakeDatePreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    private enum DB_TYPE {master, slave, rescue};

    public VmKeywordPreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();

    }

    public void start() throws Exception {
        logger.info("기준상품 키워드 전처리를 시작합니다.");
        // 필수 파라미터
        String countSql = (String) payload.getOrDefault("countSql", ""); // "SELECT COUNT(tP.prod_c) FROM tprod tP";
        String selectSql = (String) payload.getOrDefault("selectSql", "");
        String insertSql = (String) payload.getOrDefault("insertSql", "");
        String tableName = (String) payload.getOrDefault("tableName", ""); // tKeywordForSearch

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
        databaseConnector.addConn(DB_TYPE.master.name(), altibaseDriver, altibaseMasterAddress, altibaseMasterUsername, altibaseMasterPassword);
        if (altibaseSlaveEnable) {
            databaseConnector.addConn(DB_TYPE.slave.name(), altibaseDriver, altibaseSlaveAddress, altibaseSlaveUsername, altibaseSlavePassword);
        }
        if (altibaseRescueEnable) {
            databaseConnector.addConn(DB_TYPE.rescue.name(), altibaseDriver, altibaseRescueAddress, altibaseRescueUsername, altibaseRescuePassword);
        }

        try (Connection masterConnection = databaseConnector.getConn(DB_TYPE.master.name());
             Connection slaveConnection = databaseConnector.getConn(DB_TYPE.slave.name());
             Connection rescueConnection = databaseConnector.getConn(DB_TYPE.slave.name());
        )
        {
            DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();

            ResultSet tProdCountSet = databaseQueryHelper.simpleSelect(slaveConnection, countSql);
            int tprodCount = 0;
            if (tProdCountSet.next()) {
                tprodCount = tProdCountSet.getInt(1);
            }
            tProdCountSet.close();

            // 카운트가 1보다 작으면 상품이 없으므로 종료.
            if (tprodCount < 1) {
                logger.error("no prod in tProd table"); // 상품갯수가 존재하지 않아 에러
                throw new SQLException("no Prod in tProd table");
            }

            // 1. select
            long selectStart = System.currentTimeMillis(); // SELETE TIME 시작
            ResultSet resultSet = databaseQueryHelper.simpleSelectForwadOnly(altibaseSlaveEnable ? slaveConnection : masterConnection, selectSql);
            int rowCount = resultSet.getRow();
            logger.info("조회 Row 갯수: {}, SQL: {}", rowCount, selectSql.substring(0, 100));
            long selectEnd = System.currentTimeMillis(); // SELETE TIME 끝
            logger.info("SELECT {}", Utils.calcSpendTime(selectStart, selectEnd));


            // 2. truncate
            boolean isMasterTruncated = databaseQueryHelper.truncate(masterConnection, tableName);
            logger.info("[master] truncate result: {}", isMasterTruncated);
            // slave, rescue 선택적으로 truncate 호출
            boolean isSlaveTruncated = true;
            boolean isRescueTruncated = false;
            if (altibaseSlaveEnable) {
                isSlaveTruncated = databaseQueryHelper.truncate(slaveConnection, tableName);
                logger.info("[slave] truncate result: {}", isSlaveTruncated);
            }
            if (altibaseRescueEnable) {
                isRescueTruncated = databaseQueryHelper.truncate(rescueConnection, tableName);
                logger.info("[rescue] truncate result: {}", isRescueTruncated);
            }
            if (!isMasterTruncated || !isSlaveTruncated) {
                logger.warn("Truncate 실패했습니다.");
                throw new SQLException("Truncate failure");
            }


            // 3. insert
            long insertStart = System.currentTimeMillis(); // INSERT TIME 시작
            PreparedStatement preparedStatement = masterConnection.prepareStatement(insertSql);

            int addCount = 0;

            // truncate success -> insert
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("prod_c"));
                preparedStatement.setString(2, resultSet.getString("sKeywordList"));
                preparedStatement.setString(3, resultSet.getString("sBrandKeywordList"));
                preparedStatement.setString(4, resultSet.getString("sMakerKeywordList"));
                preparedStatement.setString(5, resultSet.getString("sModeKeywordListl"));

                preparedStatement.addBatch();

                addCount++;

                if (addCount % 500 == 0) { // 500개씩 실행
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);
                }
            }
            if (addCount % 500 != 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
            logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);

            long insertEnd = System.currentTimeMillis(); // INSERT TIME 끝
            logger.info("INSERT {}", Utils.calcSpendTime(insertStart, insertEnd));
            logger.info("키워드 갱신 완료! count : {}", addCount);
        }


    }
}
