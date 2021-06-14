package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.Utils;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

// 투어 전처리 객체
public class NTourPreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(NTourPreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    private final String DATABASE_ALIAS = "alti-tour";
    private final int EXECUTE_BATCH_SIZE = 100;

    public NTourPreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    public void start() throws Exception {
        logger.info("--- 색인용 여행 메뉴 테이블 구성 ---");
        // 필요한 파라미터
        String altibaseDriver = (String) payload.getOrDefault("altibaseDriver", "");
        String altibaseAddress = (String) payload.getOrDefault("altibaseAddress", "");
        String altibaseUsername = (String) payload.getOrDefault("altibaseUsername", "");
        String altibasePassword = (String) payload.getOrDefault("altibasePassword", "");

        // 쿼리
        String selectSql = (String) payload.getOrDefault("selectSql", "");
        String insertSql = (String) payload.getOrDefault("insertSql", "");
        String tableName = (String) payload.getOrDefault("tableName", "");

        DatabaseConnector databaseHandler = new DatabaseConnector();
        DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();
        databaseHandler.addConn(DATABASE_ALIAS, altibaseDriver, altibaseAddress, altibaseUsername, altibasePassword);

        try (Connection connection = databaseHandler.getConn(DATABASE_ALIAS)) {
//            갯수 로그 표시용
            int count = databaseQueryHelper.getRowCount(connection, tableName);
            logger.info("여행대표상품 갯수: {}", count);

            int deleteCount = databaseQueryHelper.delete(connection, tableName);
            logger.info("여행대표상품 데이터 삭제: {}", deleteCount);

            ResultSet resultSet = databaseQueryHelper.simpleSelect(connection, selectSql);
            int rowCount = resultSet.getRow();
            logger.info("여행대표상품 데이터 조회: {}", rowCount);

            PreparedStatement batchPreparedStatement = connection.prepareStatement(insertSql);
            int addCount = 0;
            while (resultSet.next()) {
                String col1 = resultSet.getString(1);
                String col2 = resultSet.getString(2);
                try {
                    if (col1 == null || col1.length() == 0) {
                        logger.warn("조회 데이터가 유효하지 않습니다. col1: {}", col1);
                        continue;
                    } else if (col2 == null) {
                        logger.warn("조회 데이터가 유효하지 않습니다. col2: {}", col1);
                        continue;
                    }
//                    메뉴 변환
                    String[] menuArr = Utils.decodeMenu(col2);

//                    배치 추가
                    batchPreparedStatement.setString(1, col1);
                    batchPreparedStatement.setString(2, menuArr[0]);
                    batchPreparedStatement.setString(3, menuArr[1]);
                    batchPreparedStatement.setString(4, menuArr[2]);
                    batchPreparedStatement.setString(5, menuArr[3]);
                    batchPreparedStatement.addBatch();
                    addCount ++;

                    if (addCount % EXECUTE_BATCH_SIZE == 0) {
                        batchPreparedStatement.executeBatch();
                        batchPreparedStatement.clearBatch();
                        logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);
                    }
                } catch (SQLException e1) {
                    logger.warn("", e1);
                    if("01B00".equalsIgnoreCase(e1.getSQLState())) {
                        logger.info("중복 키값 발생");
                        logger.info(e1.getMessage());
                        logger.info("col1 : " + col1 + ", col2 : " + col2);
                        logger.info("종료하지 않고 색인 진행");
                    } else {
                        logger.error("여행 메뉴 테이블 갱신 실패. --- state: {}", e1.getSQLState());
                        logger.error("", e1);
                        throw e1;
                    }
//                    TODO 전처리 실패시 알림기능.
                } catch (Exception e2) {
                    logger.error("", e2);
                    throw e2;
                }
            }
            // 나머지 처리.
            if (addCount % EXECUTE_BATCH_SIZE != 0) {
                try {
                    batchPreparedStatement.executeBatch();
                    batchPreparedStatement.clearBatch();
                    logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);
                } catch (SQLException e1) {
                    logger.warn("", e1);
                    if("01B00".equalsIgnoreCase(e1.getSQLState())) {
                        logger.info("중복 키값 발생");
                        logger.info(e1.getMessage());
                        logger.info("종료하지 않고 색인 진행");
                    } else {
                        logger.error("여행 메뉴 테이블 갱신 실패. --- state: {}", e1.getSQLState());
                        logger.error("", e1);
                    }
                } catch (Exception e) {
                    logger.warn("", e);
                }
            }


            int afterCount = databaseQueryHelper.getRowCount(connection, tableName);
            logger.info("여행 대표 상품 갯수(갱신 후) : " + afterCount);

        }
    }
}
