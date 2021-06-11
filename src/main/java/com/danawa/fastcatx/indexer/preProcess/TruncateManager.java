package com.danawa.fastcatx.indexer.preProcess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class TruncateManager {
    private static final Logger logger = LoggerFactory.getLogger(TruncateManager.class);

    public boolean linkTruncateCall(String env, String dbTable, String alti_master_url, String alti_slave_url, String alti_rescue_url, String alti_user, String alti_password) {
        Connection connection = null;
        CallableStatement cs = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String selectQuery = "";

        if(dbTable.equals("tKeywordForSearch")) {	// 링크 데이터
            selectQuery = "SELECT COUNT(NPRODUCTSEQ) FROM " + dbTable;
        }else if(dbTable.equals("tCategoryForSearch")){ // 링크 & 미링크 데이터
            selectQuery = "SELECT COUNT(NCATEGORYSEQ1) FROM " + dbTable;
        }else if(dbTable.equals("tFirstDateForSearch")){
            selectQuery = "SELECT COUNT(prod_c) FROM " + dbTable;
        }

        Boolean isEmpty = false;
        int startCnt = 1;
        int endCnt = 1;

        String[] result = new String[3];
        int serverCnt=0;
        if("operating".equals(env)){
            serverCnt = 3;
        }else{
            serverCnt = 1;
        }

        try{
            //alti1-dev = 1
            //운영 = 3
            String dbServerName = "";

            for(int i=0; i<serverCnt; i++) {
                if(i==0) {
                    dbServerName = "MASTER";
                    connection = DriverManager.getConnection(alti_master_url, alti_user, alti_password);
                }else if(i==1){
                    dbServerName = "SLAVE";
                    connection = DriverManager.getConnection(alti_slave_url, alti_user, alti_password);
                }else{
                    dbServerName = "RESCUE";
                    connection = DriverManager.getConnection(alti_rescue_url, alti_user, alti_password);
                }

                logger.info("{} server TRUNCATE start ({}/{})",dbServerName, startCnt++, serverCnt);

                cs = connection.prepareCall("{CALL PRTRUNCATE(?, ?)}");	// 프로시저 호출 쿼리(알티)
                cs.setString(1, dbTable); 				// IN : truncate할 테이블
                cs.registerOutParameter(2, Types.CHAR); // OUT : 결과
                cs.execute(); 							// 프로시저 실행 tCategoryForSearch 테이블 내용 삭제

                logger.info("{} server TRUNCATE end ({}/{})",dbServerName, endCnt++, serverCnt);

                Thread.sleep(5000);

                pstmt = connection.prepareStatement(selectQuery, ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
                rs =  pstmt.executeQuery();



                if(rs.next()) {
                    if(rs.getInt(1) == 0) {
                        result[i] = "0";
                    }else{
                        result[i] = "1";
                    }
                }

                Thread.sleep(5000);
            }

            Integer resultCnt = 0;
            for(int i=0;i<serverCnt;i++){
                resultCnt += Integer.parseInt(result[i]);
            }
            if(resultCnt == 0) {
                isEmpty = true;
            }else{
                logger.error("TRUNCATE fail : LINK - {}", dbServerName);
//                sms.sendSMS("TRUNCATE 수행이 안된 서버가 있습니다 : " + server + "-" + dbServerName);
            }

            return isEmpty;

        } catch (Exception e){
            logger.error("truncate sql exception error ", e.getMessage());
//            sms.sendSMS("TRUNCATE 프로시저 호출 중 에러 발생 : " + e.getMessage());
        } finally{
            try {
                rs.close();
                pstmt.close();
                connection.close();
            } catch (Exception e3) {
                logger.error("connection close fail");
            }
        }

        return isEmpty;
    }

}
