package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.Utils;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VmFirstMakeDatePreProcess {
    private static final Logger logger = LoggerFactory.getLogger(VmFirstMakeDatePreProcess.class);
    private Job job;
    private String dataSQL;

    public VmFirstMakeDatePreProcess(Job job, String dataSQL) {
        this.job = job;
        this.dataSQL = dataSQL;
    }

    public void start() {
        // select
        long selectStart = System.currentTimeMillis(); // SELETE TIME 시작

        long selectEnd = System.currentTimeMillis(); // SELETE TIME 끝
        logger.info("SELECT {}", Utils.calcSpendTime(selectStart, selectEnd));
        // insert


    }
}
