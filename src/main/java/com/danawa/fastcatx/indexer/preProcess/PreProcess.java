package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public interface PreProcess {

       static PreProcess startProcess(String className, Job job) throws Exception {

        String classFullName = "com.danawa.fastcatx.indexer.preProcess." + className;
        Class klass = Class.forName(classFullName);
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
        String className = (String) payload.getOrDefault("type", "");
        startProcess(className, job);
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
