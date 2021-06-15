package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;

public class CategoryKeywordPreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(CategoryKeywordPreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    private enum DB_TYPE {master, slave, rescue}

    public CategoryKeywordPreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("카테고리 키워드 전처리를 시작합니다.");
        // 필수 파라미터
        String selectSql = (String) payload.getOrDefault("selectSql", "");
        String insertSql = (String) payload.getOrDefault("insertSql", "");
        String tableName = (String) payload.getOrDefault("tableName", ""); // tCategoryForSearch

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
        try (
                Connection masterConnection = databaseConnector.getConn(DB_TYPE.master.name());
                Connection slaveConnection = databaseConnector.getConn(DB_TYPE.slave.name());
                Connection rescueConnection = databaseConnector.getConn(DB_TYPE.rescue.name());
        ) {
            DatabaseQueryHelper databaseQueryHelper = new DatabaseQueryHelper();

//            현재 테이블 조회
            ResultSet resultSet = databaseQueryHelper.simpleSelect(altibaseSlaveEnable ? slaveConnection : masterConnection, selectSql);
            int rowCount = 0;
            if (resultSet.last()) {
                rowCount = resultSet.getRow();
                resultSet.beforeFirst();
            }
            logger.info("조회 Row 갯수: {}, SQL: {}", rowCount, selectSql.substring(0, 100));

//            truncate 프로시저 호출
            boolean isMasterTruncated = databaseQueryHelper.truncate(masterConnection, tableName);
            logger.info("[master] truncate result: {}", isMasterTruncated);
            // slave, rescue 선택적으로 truncate 호출
            boolean isSlaveTruncated = !altibaseSlaveEnable || databaseQueryHelper.truncate(slaveConnection, tableName);
            boolean isRescueTruncated = !altibaseRescueEnable || databaseQueryHelper.truncate(rescueConnection, tableName);
            logger.info("[slave] truncate result: {}", isSlaveTruncated);
            logger.info("[rescue] truncate result: {}", isRescueTruncated);
            if (!isMasterTruncated || !isSlaveTruncated || !isRescueTruncated) {
                logger.warn("Truncate 실패했습니다.");
                throw new SQLException("Truncate failure");
            }

//            데이터 추가
//            // |CATE_C1|CATE_C2|CATE_C3|CATE_C4|CATE_N1|CATE_N2|CATE_N3|CATE_N4|DISP_YN|VIRTUAL_YN|CATE_K1
//            // |CATE_K2|CATE_K3|CATE_K4|DEPTH|BRPS|
//            // |1 |0 |0 |0 |주방가전 | | | |Y |N |2^주방가전, 2^ 키친, 2^ 용품| | | |1 |S |
            PreparedStatement preparedStatement = masterConnection.prepareStatement(insertSql);

            int addCount = 0;
            while (resultSet.next()) {
                String[] categories = getCategories(resultSet);

                HashSet<String> categoryWeightSet = new HashSet<>();
                HashSet<String> categoryKeywordSet = new HashSet<>();

                for (String category : categories) {
                    category = category
                            .trim()
                            .replaceAll("\\^ ", "\\^"); // 구분기호랑 키워드간의 띄어쓰기 제거 ex) 1^ 김장 ->

                    // 1^김장
                    String tmpFirstChar = category.substring(0, 1); // 맨앞 숫자만 읽은 후 저장

                    if (category.length() >= 2) {
                        category = category.substring(2); // 1^ 이후 부터 끝까지 읽음(키워드만 저장)
                    }

                    if (tmpFirstChar.equals("1")) { // 숫자가 1이면
                        categoryWeightSet.add(category); // 가중치키워드+검색키워드
                    }

                    if (!category.equals("^")) {
                        categoryKeywordSet.add(category);
                    }
                }
                String categoryKeyword = new String();
                String categoryWeight = new String();

                if (!categoryWeightSet.isEmpty()) {
                    categoryWeight = categoryWeightSet
                            .toString()
                            .replaceAll("[\\[\\]]", "")
                            .replaceAll(", ", ",");
                }

                if (!categoryKeywordSet.isEmpty()) {
                    categoryKeyword = categoryKeywordSet
                            .toString()
                            .replaceAll("[\\[\\]]", "")
                            .replaceAll(", ", ",");
                }

                preparedStatement.setInt(1, resultSet.getInt("cate_c1"));
                preparedStatement.setInt(2, resultSet.getInt("cate_c2"));
                preparedStatement.setInt(3, resultSet.getInt("cate_c3"));
                preparedStatement.setInt(4, resultSet.getInt("cate_c4"));
                preparedStatement.setString(5, resultSet.getString("cate_n1"));
                preparedStatement.setString(6, resultSet.getString("cate_n2"));
                preparedStatement.setString(7, resultSet.getString("cate_n3"));
                preparedStatement.setString(8, resultSet.getString("cate_n4"));
                preparedStatement.setString(9, resultSet.getString("disp_yn"));
                preparedStatement.setString(10, resultSet.getString("virtual_yn"));
                preparedStatement.setString(11, categoryKeyword);
                preparedStatement.setString(12, categoryWeight);
                preparedStatement.setString(13, resultSet.getString("BRPS"));
                preparedStatement.addBatch();

                addCount++;
                if (addCount % 100 == 0) {
                    preparedStatement.executeBatch();
                    preparedStatement.clearBatch();
                    logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);
                }
            }

            if (addCount % 100 != 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
            logger.info("데이터를 추가하였습니다. {} / {}", addCount, rowCount);
        }

        logger.info("카테고리키워드 전처리를 완료하였습니다.");
    }

    private String[] getCategories(ResultSet resultSet) throws SQLException {
        int depth = Integer.parseInt(resultSet.getString("depth"));

        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= depth; i++) {
            sb.append(resultSet.getString("cate_k" + i));
            if (i < depth) {
                sb.append(", ");
            }
        }

        return sb.toString().split(", ");
    }

}
