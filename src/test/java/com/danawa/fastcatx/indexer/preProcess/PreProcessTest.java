package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.IndexJobRunner;
import com.danawa.fastcatx.indexer.entity.Job;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.scheduling.support.CronTrigger;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@SpringBootTest
public class PreProcessTest {
    private static final Logger logger = LoggerFactory.getLogger(PreProcessTest.class);

    @Test
    public void nTourTest() {
        Job job = new Job();

        try {
//        // 쿼리
        String selectSql = "SELECT /*+ LEADING (TMTP) */\n" +
                "DISTINCT\n" +
                "        TMTP.SMAINTOURPRODUCTCODE as MAINPRODUCTCODE,\n" +
                "        GROUP_CONCAT(DISTINCT(TTM.fullMunuSeq), ',') as FULLSEQ\n" +
                "FROM TMAINTOURPRODUCT TMTP\n" +
                "        INNER JOIN TLINKTOURMAINPRODUCTCATEGORY TLTMPC on (TLTMPC.SMAINTOURPRODUCTCODE = TMTP.SMAINTOURPRODUCTCODE AND TLTMPC.SDISPLAYYN='Y')\n" +
                "        LEFT JOIN TTOURMENUOPTIONVALUE TTMOV ON (TLTMPC.NTOURCATEGORYSEQ = TTMOV.NTOURCATEGORYSEQ)\n" +
                "        LEFT JOIN (SELECT b.NTOURMENUSEQ, SUBSTR (SYS_CONNECT_BY_PATH (b.NTOURMENUSEQ, '>'), 2) as fullMunuSeq\n" +
                "                        FROM TTOURMENU b\n" +
                "                        START WITH b.NTOURMENUSEQ = 1\n" +
                "                        CONNECT BY PRIOR b.NTOURMENUSEQ = b.NPARENTMENUSEQ ) as TTM ON (TTMOV.NTOURMENUSEQ = TTM.NTOURMENUSEQ )\n" +
                "group by TMTP.SMAINTOURPRODUCTCODE;";
        String insertSql = "" +
                "INSERT INTO TMAINTOURPRODUCTMENU " +
                "           (SMAINTOURPRODUCTCODE,STOURMENUSEQ2,STOURMENUSEQ3,STOURMENUSEQ4,STOURMENUSEQ5) " +
                "     VALUES (?, ?, ?, ?, ?)";

            Map<String, Object> payload  = new HashMap<>();
            payload.put("altibaseDriver", "Altibase.jdbc.driver.AltibaseDriver");
            payload.put("altibaseAddress", "jdbc:Altibase://altitour-dev.danawa.io:20200/DNWTOUR");
//            payload.put("altibaseAddress", "jdbc:Altibase://es2.danawa.io:30031/DNWTOUR");
//            payload.put("altibaseUsername", "DBNTOUR_A");
//            payload.put("altibasePassword", "qnxmfhtm#^^");
            payload.put("altibaseUsername", "DBNTOUR_A1");
            payload.put("altibasePassword", "qnxmfhtm#^^1");
            payload.put("selectSql", selectSql);
            payload.put("insertSql", insertSql);
            payload.put("tableName", "TMAINTOURPRODUCTMENU");


            job.setStartTime(System.currentTimeMillis());
            job.setId(UUID.randomUUID());
            job.setAction(IndexJobRunner.STATUS.READY.name());
            job.setRequest(payload);

            new NTourPreProcess(job).start();
        } catch (Exception e) {
            logger.error("", e);
        }

        logger.info("{}", job);
    }


    @Test
    public void categoryKeyword() {
        Job job = new Job();
        try {

//        // 쿼리
            String selectSql = "SELECT\n" +
                    "        tC1.cate_c cate_c1,\n" +
                    "        0 cate_c2,\n" +
                    "        0 cate_c3,\n" +
                    "        0 cate_c4,\n" +
                    "        tC1.cate_n cate_n1,\n" +
                    "        '' cate_n2,\n" +
                    "        '' cate_n3,\n" +
                    "        '' cate_n4,\n" +
                    "        tC1.disp_yn disp_yn,\n" +
                    "        'N' virtual_yn,\n" +
                    "        GROUP_CONCAT(tCK1.emType || '^' || tCK1.sCategoryKeyword , ', ') as cate_k1,\n" +
                    "        '' cate_k2,\n" +
                    "        '' cate_k3,\n" +
                    "        '' cate_k4,\n" +
                    "        1 depth,\n" +
                    "        tC1.emBundleRepresentProductSetting BRPS\n" +
                    "FROM\n" +
                    "        tcate tC1\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK1 ON (tC1.cate_c = tCK1.cate_c)\n" +
                    "WHERE\n" +
                    "        tC1.depth=1\n" +
                    "GROUP BY\n" +
                    "        tC1.cate_c, tC1.cate_n, tC1.disp_yn, tC1.emBundleRepresentProductSetting\n" +
                    "UNION\n" +
                    "SELECT\n" +
                    "        tC1.cate_c cate_c1,\n" +
                    "        tC2.cate_c cate_c2,\n" +
                    "        0 cate_c3,\n" +
                    "        0 cate_c4,\n" +
                    "        tC1.cate_n cate_n1,\n" +
                    "        tC2.cate_n cate_n2,\n" +
                    "        '' cate_n3,\n" +
                    "        '' cate_n4,\n" +
                    "        tC2.disp_yn disp_yn,\n" +
                    "        'N' virtual_yn,\n" +
                    "        GROUP_CONCAT(tCK1.emType || '^' || tCK1.sCategoryKeyword , ', ') as cate_k1,\n" +
                    "        GROUP_CONCAT(tCK2.emType || '^' || tCK2.sCategoryKeyword , ', ') as cate_k2,\n" +
                    "        '' cate_k3,\n" +
                    "        '' cate_k4,\n" +
                    "        2 depth,\n" +
                    "        tC2.emBundleRepresentProductSetting BRPS\n" +
                    "FROM\n" +
                    "        tcate tC1\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK1 ON (tC1.cate_c = tCK1.cate_c),\n" +
                    "        tcate tC2\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK2 ON (tC2.cate_c = tCK2.cate_c)\n" +
                    "WHERE\n" +
                    "        tC2.pcate_c=tC1.cate_c\n" +
                    "        AND tC2.depth=2\n" +
                    "GROUP BY\n" +
                    "        tC1.cate_c, tC2.cate_c, tC1.cate_n, tC2.cate_n, tC2.disp_yn, tC2.emBundleRepresentProductSetting\n" +
                    "UNION\n" +
                    "SELECT\n" +
                    "        tC1.cate_c cate_c1,\n" +
                    "        tC2.cate_c cate_c2,\n" +
                    "        tC3.cate_c cate_c3,\n" +
                    "        0 cate_c4,\n" +
                    "        tC1.cate_n cate_n1,\n" +
                    "        tC2.cate_n cate_n2,\n" +
                    "        tC3.cate_n cate_n3,\n" +
                    "        '' cate_n4,\n" +
                    "        tC3.disp_yn disp_yn,\n" +
                    "        'N' virtual_yn,\n" +
                    "        GROUP_CONCAT(tCK1.emType || '^' || tCK1.sCategoryKeyword , ', ') as cate_k1,\n" +
                    "        GROUP_CONCAT(tCK2.emType || '^' || tCK2.sCategoryKeyword , ', ') as cate_k2,\n" +
                    "        GROUP_CONCAT(tCK3.emType || '^' || tCK3.sCategoryKeyword , ', ') as cate_k3,\n" +
                    "        '' cate_k4,\n" +
                    "        3 depth,\n" +
                    "        tC3.emBundleRepresentProductSetting BRPS\n" +
                    "FROM\n" +
                    "        tcate tC1\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK1 ON (tC1.cate_c = tCK1.cate_c),\n" +
                    "        tcate tC2\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK2 ON (tC2.cate_c = tCK2.cate_c),\n" +
                    "        tcate tC3\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK3 ON (tC3.cate_c = tCK3.cate_c)\n" +
                    "WHERE\n" +
                    "        tC3.pcate_c=tC2.cate_c\n" +
                    "        AND tC2.pcate_c=tC1.cate_c\n" +
                    "        AND tC3.depth=3\n" +
                    "GROUP BY\n" +
                    "        tC1.cate_c, tC2.cate_c, tC3.cate_c, tC1.cate_n, tC2.cate_n, tC3.cate_n, tC3.disp_yn, tC3.emBundleRepresentProductSetting\n" +
                    "UNION\n" +
                    "SELECT\n" +
                    "        tC1.cate_c cate_c1,\n" +
                    "        tC2.cate_c cate_c2,\n" +
                    "        tC3.cate_c cate_c3,\n" +
                    "        tC4.cate_c cate_c4,\n" +
                    "        tC1.cate_n cate_n1,\n" +
                    "        tC2.cate_n cate_n2,\n" +
                    "        tC3.cate_n cate_n3,\n" +
                    "        tC4.cate_n cate_n4,\n" +
                    "        tC4.disp_yn disp_yn,\n" +
                    "        'N' virtual_yn,\n" +
                    "        GROUP_CONCAT(tCK1.emType || '^' || tCK1.sCategoryKeyword , ', ') as cate_k1,\n" +
                    "        GROUP_CONCAT(tCK2.emType || '^' || tCK2.sCategoryKeyword , ', ') as cate_k2,\n" +
                    "        GROUP_CONCAT(tCK3.emType || '^' || tCK3.sCategoryKeyword , ', ') as cate_k3,\n" +
                    "        GROUP_CONCAT(tCK4.emType || '^' || tCK4.sCategoryKeyword , ', ') as cate_k4,\n" +
                    "        4 depth,\n" +
                    "        tC4.emBundleRepresentProductSetting BRPS\n" +
                    "FROM\n" +
                    "        tcate tC1\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK1 ON (tC1.cate_c = tCK1.cate_c),\n" +
                    "        tcate tC2\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK2 ON (tC2.cate_c = tCK2.cate_c),\n" +
                    "        tcate tC3\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK3 ON (tC3.cate_c = tCK3.cate_c),\n" +
                    "        tcate tC4\n" +
                    "        LEFT JOIN tSearchKeywordCategory tCK4 ON (tC4.cate_c = tCK4.cate_c)\n" +
                    "WHERE\n" +
                    "        tC4.pcate_c=tC3.cate_c\n" +
                    "        AND     tC3.pcate_c=tC2.cate_c\n" +
                    "        AND tC2.pcate_c=tC1.cate_c\n" +
                    "        AND tC4.depth=4\n" +
                    "GROUP BY\n" +
                    "        tC1.cate_c, tC2.cate_c, tC3.cate_c, tC4.cate_c, tC1.cate_n, tC2.cate_n, tC3.cate_n, tC4.cate_n, tC4.disp_yn, tC4.emBundleRepresentProductSetting";
            String insertSql = "INSERT INTO TCATEGORYFORSEARCH " +
                    "(NCATEGORYSEQ1,NCATEGORYSEQ2,NCATEGORYSEQ3,NCATEGORYSEQ4,SCATEGORYNAME1,SCATEGORYNAME2,SCATEGORYNAME3,SCATEGORYNAME4,SDISPYN,SVIRTUALYN,SCATEGORYKEYWORDLIST,SCATEGORYWEIGHTLIST,SBRPS) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

            Map<String, Object> payload  = new HashMap<>();
            payload.put("selectSql", selectSql);
            payload.put("insertSql", insertSql);
            payload.put("tableName", "tCategoryForSearch");

            payload.put("altibaseDriver", "Altibase.jdbc.driver.AltibaseDriver");
            payload.put("altibaseMasterAddress", "jdbc:Altibase://es2.danawa.io:30032/DNWALTI?SessionFailOver=off&ConnectionRetryCount=2&ConnectionRetryDelay=0&LoadBalance=off");
            payload.put("altibaseMasterUsername", "DBLINKDATA_A");
            payload.put("altibaseMasterPassword", "ektbfm#^^");

            payload.put("altibaseSlaveEnable", false);
            payload.put("altibaseSlaveAddress", "jdbc:Altibase://es2.danawa.io:30032/DNWALTI?SessionFailOver=off&ConnectionRetryCount=2&ConnectionRetryDelay=0&LoadBalance=off");
            payload.put("altibaseSlaveUsername", "DB");
            payload.put("altibaseSlavePassword", "PW");

            payload.put("altibaseRescueEnable", false);
            payload.put("altibaseRescueAddress", "jdbc:Altibase://es2.danawa.io:30032/DNWALTI?SessionFailOver=off&ConnectionRetryCount=2&ConnectionRetryDelay=0&LoadBalance=off");
            payload.put("altibaseRescueUsername", "DB");
            payload.put("altibaseRescuePassword", "PW");

            job.setStartTime(System.currentTimeMillis());
            job.setId(UUID.randomUUID());
            job.setAction(IndexJobRunner.STATUS.READY.name());
            job.setRequest(payload);

            new CategoryKeywordPreProcess(job).start();
        } catch (Exception e) {
            logger.error("", e);
        }
        logger.info("{}", job);

    }

}
