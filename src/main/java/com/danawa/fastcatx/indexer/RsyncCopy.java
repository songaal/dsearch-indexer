package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.output.LogOutPutProcessOutput;
import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.processoutput4j.output.ConsoleOutputProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

import java.io.File;

public class RsyncCopy extends Thread {


    Logger logger = LoggerFactory.getLogger(RsyncCopy.class);

    private String rsyncIp;
    private String rsyncPath;
    private Integer groupSeq;
    private String bwlimit;
    private boolean isCopy = true;

    public RsyncCopy(String rsyncIp, String rsyncPath, String bwlimit, Integer groupSeq) {
        this.rsyncIp = rsyncIp;
        this.rsyncPath = rsyncPath;
        this.groupSeq = groupSeq;
        this.bwlimit = bwlimit;
    }

    public boolean copyAsync() {
        return isCopy;
    }

    public void run() {

        logger.info("path : {}", rsyncPath);
        logger.info("bwlimit : {}", bwlimit);
        String rsyncFileName = "prodExt_"+groupSeq;
        File file = new File(rsyncPath +"/"+rsyncFileName);
        //File file = new File(rsyncPath +"\\"+rsyncFileName);

        if (file.exists()) {
            logger.info("기존 파일 삭제 : {}", file);
            file.delete();
        }
        RSync rsync = new RSync()
                //.source("C:\\Users\\admin\\Desktop\\indexFile\\sample\\prodExt_5")
                .source(rsyncIp+"::search_data_alti/"+rsyncFileName)
                .destination(rsyncPath)
                .recursive(true)
                //.progress(true)
                .archive(true)
                .compress(true)
                .bwlimit(bwlimit)
                .inplace(true);

        LogOutPutProcessOutput output = new LogOutPutProcessOutput();
        try {
            output.monitor(rsync.builder());
        } catch (Exception e) {
            logger.error("Rsync Exception : {}", e);
            isCopy = false;
            throw new RuntimeException(e);
        }
    }

}
