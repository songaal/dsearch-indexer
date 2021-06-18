package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public interface PreProcess {
    enum TYPE { NTOUR, CATEGORY_KEYWORD, CATEGORY, VM_KEYWORD, VM_FIRSTMAKE_DATE, ACKEYWORD, SHOP_DNW_ACK, POPULARITY_SCORE, MOBILE }

    default void starter(Job job) throws Exception {
        Map<String, Object> payload = job.getRequest();
        String type = (String) payload.getOrDefault("type", "");
        if (TYPE.NTOUR.name().equalsIgnoreCase(type)) {
            new NTourPreProcess(job).start();
        } else if (TYPE.CATEGORY_KEYWORD.name().equalsIgnoreCase(type)) {
            new CategoryKeywordPreProcess(job).start();
        } else if (TYPE.CATEGORY.name().equalsIgnoreCase(type)) {
            new CategoryPreProcess(job).start();
        } else if (TYPE.ACKEYWORD.name().equalsIgnoreCase(type)) {
            new AcKeywordPreProcess(job).start();
        } else if (TYPE.VM_KEYWORD.name().equalsIgnoreCase(type)) {
            new VmKeywordPreProcess(job).start();
        } else if (TYPE.VM_FIRSTMAKE_DATE.name().equalsIgnoreCase(type)) {
            new VmFirstMakeDatePreProcess(job).start();
        } else if (TYPE.SHOP_DNW_ACK.name().equalsIgnoreCase(type)) {
            new ShopDnwAckPreProcess(job).start();
        } else if (TYPE.POPULARITY_SCORE.name().equalsIgnoreCase(type)) {
            new PopularityScorePreProcess(job).start();
        } else if (TYPE.MOBILE.name().equalsIgnoreCase(type)) {
            new MobilePreProcess(job).start();
        }
    }

    void start() throws Exception;

    class EmptyPreProcess implements PreProcess {
        private static final Logger logger = LoggerFactory.getLogger(EmptyPreProcess.class);
        @Override
        public void start() {
            logger.info("Empty !!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }
}
