package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public interface PreProcess {
    enum TYPE { NTOUR, CATEGORY_KEYWORD, CATEGORY, VM_KEYWORD, VM_FIRSTMAKE_DATE, ACKEYWORD, SHOP_DNW_ACK, POPULARITY_SCORE, MOBILE, POPULARITY_SCORE_UNLINK, CALL_URL  }

    String[][] classNameMap = {
            {TYPE.NTOUR.name(), "com.danawa.fastcatx.indexer.preProcess.NTourPreProcess"},
            {TYPE.CATEGORY_KEYWORD.name(), "com.danawa.fastcatx.indexer.preProcess.CategoryKeywordPreProcess"},
            {TYPE.CATEGORY.name(), "com.danawa.fastcatx.indexer.preProcess.CategoryPreProcess"},
            {TYPE.VM_KEYWORD.name(), "com.danawa.fastcatx.indexer.preProcess.VMKeywordPreProcess"},
            {TYPE.VM_FIRSTMAKE_DATE.name(), "com.danawa.fastcatx.indexer.preProcess.VMFirstMakeDatePreProcess"},
            {TYPE.ACKEYWORD.name(), "com.danawa.fastcatx.indexer.preProcess.ACKeywordPreProcess"},
            {TYPE.SHOP_DNW_ACK.name(), "com.danawa.fastcatx.indexer.preProcess.ShopDnwACKeywordPreProcess"},
            {TYPE.POPULARITY_SCORE.name(), "com.danawa.fastcatx.indexer.preProcess.PopularityScorePreProcess"},
            {TYPE.MOBILE.name(), "com.danawa.fastcatx.indexer.preProcess.MobilePreProcess"},
            {TYPE.POPULARITY_SCORE_UNLINK.name(), "com.danawa.fastcatx.indexer.preProcess.PopularityScoreUnlinkPreProcess"},
            {TYPE.CALL_URL.name(), "com.danawa.fastcatx.indexer.preProcess.CallUrlPreProcess"}
    };

    static String getClassName(TYPE type) {
        return getClassName(type.name());
    }

    static String getClassName(String typeName) {
        for (String[] map : classNameMap) {
            if (map[0].equalsIgnoreCase(typeName)) {
                return map[1];
            }
        }
        return null;
    }

    static PreProcess startProcess(String typeName, Job job) throws Exception {
        String className = getClassName(typeName);
        Class klass = null;
        if (className != null) {
            klass = Class.forName(className);
        }
        Object instance = null;
        if(klass != null) {
            instance = klass.getConstructor(Job.class).newInstance(job);
            klass.getMethod("start").invoke(instance);
        }
        return (PreProcess) instance;
    }

    /**
     * Job 을 시작시킨다.
     * */
    default void starter(Job job) throws Exception {
        Map<String, Object> payload = job.getRequest();
        String type = (String) payload.getOrDefault("type", "");
        startProcess(type, job);
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
