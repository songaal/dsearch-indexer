package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PopularityScorePreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(PopularityScorePreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    public PopularityScorePreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("인기점수 전처리를 시작합니다.");
        // 파라미터
        String logDBDriver = (String) payload.getOrDefault("logDBDriver", "");
        String logDBAddress = (String) payload.getOrDefault("logDBAddress", "");
        String logDBUsername = (String) payload.getOrDefault("logDBUsername", "");
        String logDBPassword = (String) payload.getOrDefault("logDBPassword", "");
//        remakePopularityScore_new.sql
        String selectSql = (String)  payload.getOrDefault("selectSql", "");

        String altiMasterDBDriver = (String) payload.getOrDefault("altiMasterDBDriver", "");
        String altiMasterDBAddress = (String) payload.getOrDefault("altiMasterDBAddress", "");
        String altiMasterDBUsername = (String) payload.getOrDefault("altiMasterDBUsername", "");
        String altiMasterDBPassword = (String) payload.getOrDefault("altiMasterDBPassword", "");


        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn("logDB", logDBDriver, logDBAddress, logDBUsername, logDBPassword);
        databaseConnector.addConn("master", altiMasterDBDriver, altiMasterDBAddress, altiMasterDBUsername, altiMasterDBPassword);

        // 1. 검색상품 클릭 로그 데이터 가져오기 - mysql
        // 2. 검색상품 인기점수 데이터 비우기 - altibase
        // 3. 검색상품 인기점수 데이터 채우기 - altibase
        Map<String, Integer> mapPopularityScore = new HashMap<>();

        try (Connection connection = databaseConnector.getConn("logDB")) {
            logger.debug("mysql tPVShopItemBuyClick 수집 시작.");

            long queryStartTime = System.currentTimeMillis(); // 쿼리 시작 시간
            PreparedStatement preparedStatement = connection.prepareStatement(selectSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet resultSet = preparedStatement.executeQuery(); // 쿼리문 전송
            long queryEndTime = System.currentTimeMillis(); // 쿼리 종료 시간
            logger.debug("mysql tPVShopItemBuyClick 쿼리 시간 : " + (queryEndTime - queryStartTime));

            double queryTime = (queryEndTime - queryStartTime) / (double) 1000;
            int minute = (int) queryTime / 60;
            int second = (int) queryTime % 60;
            logger.debug("mysql tPVShopItemBuyClick 수집 시간 : " + minute + " min" + " " + second + " sec");

            resultSet.last(); // 커서를 맨아래로
            int recodeCount = resultSet.getRow(); // 데이터 개수 체크
            resultSet.beforeFirst(); // 커서를 맨위로
            while (resultSet.next()) {
                String cmpny_c = resultSet.getString("sShopId");
                String link_prod_c = resultSet.getString("sShopItemId");
                int pcScore = resultSet.getInt("nPcNormalCount");
                int mobileScore = resultSet.getInt("nMobileWebNormalCount");
                int score = pcScore + mobileScore;
                if (cmpny_c.length() > 6) {
                    cmpny_c = cmpny_c.substring(0, 6);
                }
                boolean pattern_cmpny_c = Pattern.matches("^[a-zA-Z0-9]*$", cmpny_c);
                boolean pattern_link_prod_c = Pattern.matches("^[a-zA-Z0-9]*$", link_prod_c);

                if (pattern_cmpny_c && pattern_link_prod_c) {
                    String key = cmpny_c + "_" + link_prod_c;
                    Integer tempScore = mapPopularityScore.get(key);
                    if (tempScore == null) {
                        mapPopularityScore.put(key, score);
                    } else if (tempScore != null) {
                        mapPopularityScore.put(key, score + tempScore);
                    }
                }
            }
            logger.info("제휴상품 인기점수 갱신 전 개수:" + recodeCount);
            logger.info("제휴상품 인기점수 갱신 후 개수:" + mapPopularityScore.size());
            logger.debug("mysql tPVShopItemBuyClick 수집 종료.");
        } catch (Exception e) {
            logger.error("", e);
//            TODO 알림처리
            throw e;
        }

        try (Connection connection = databaseConnector.getConn("master")) {
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 삭제 시작.");
            String deleteQuery = "DELETE FROM TCMPNY_LINK_POPULARITY_SCORE";
            long queryStartTime = System.currentTimeMillis(); // 쿼리 시작 시간
            Statement statement = connection.createStatement();
            statement.executeUpdate(deleteQuery);
            long queryEndTime = System.currentTimeMillis(); // 쿼리 종료 시간
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 쿼리 시간 : " + (queryEndTime - queryStartTime));
            double queryTime = (queryEndTime - queryStartTime) / (double) 1000;
            int minute = (int) queryTime / 60;
            int second = (int) queryTime % 60;
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 삭제 시간 : " + minute + " min" + " " + second + " sec");
        } catch (Exception e) {
            logger.error("", e);
//            TODO 알림 처리
            throw e;
        }

        int totalCount = 0;
        try (Connection connection = databaseConnector.getConn("master")) {
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 갱신 시작.");
            long queryStartTime = System.currentTimeMillis(); // 쿼리 시작 시간
            String insertQuery = "INSERT INTO TCMPNY_LINK_POPULARITY_SCORE (CMPNY_C, LINK_PROD_C, NPOPULARITY_SCORE) VALUES (?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
            Set<String> set = mapPopularityScore.keySet();
            for (String key : set) {
                String temp[] = key.split("_");
                int score = mapPopularityScore.get(key);
                preparedStatement.clearParameters(); // 파라미터 초기화
                preparedStatement.setString(1, temp[0]);
                preparedStatement.setString(2, temp[1]);
                preparedStatement.setInt(3, score);

                preparedStatement.addBatch(); // 배치에 담음
                totalCount++;
                if (totalCount % 100 == 0) { // 100개씩 실행
                    preparedStatement.executeBatch(); // 배치 실행
                    preparedStatement.clearBatch(); // 배치 초기화
                }
            }

            if (totalCount % 100 != 0) { // 나머지 배치 실행
                preparedStatement.executeBatch(); // 배치 실행
                preparedStatement.clearBatch(); // 배치 초기화
            }
            long queryEndTime = System.currentTimeMillis(); // 쿼리 종료 시간
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 쿼리 시간 : " + (queryEndTime - queryStartTime));
            double queryTime = (queryEndTime - queryStartTime) / (double) 1000;
            int minute = (int) queryTime / 60;
            int second = (int) queryTime % 60;
            logger.debug("TCMPNY_LINK_POPULARITY_SCORE 갱신 시간 : " + minute + " min" + " " + second + " sec");
        } catch (Exception e) {
            logger.error("", e);
//            TODO 알림 처리
            throw e;
        }

        logger.info("인기점수 전처리 완료하였습니다.");
    }
}
