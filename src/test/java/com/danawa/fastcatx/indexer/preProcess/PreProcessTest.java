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

}
