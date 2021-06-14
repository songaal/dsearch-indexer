package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

public interface PreProcess {
    enum TYPE { NTOUR, CATEGORY_KEYWORD, VM_KEYWORD, VM_FIRSTMAKE_DATE }

    default void starter(Job job) throws SQLException, ClassNotFoundException {
        Map<String, Object> payload = job.getRequest();
        String type = (String) payload.getOrDefault("type", "");
        String dataSQL = (String) payload.get("dataSQL");
        String env = (String) payload.get("env");


        if (TYPE.NTOUR.name().equalsIgnoreCase(type)) {
            new NTourPreProcess(job).start();
        } else if (TYPE.CATEGORY_KEYWORD.name().equalsIgnoreCase(type)) {

        } else if (TYPE.VM_KEYWORD.name().equalsIgnoreCase(type)) {

        } else if (TYPE.VM_FIRSTMAKE_DATE.name().equalsIgnoreCase(type)) {
            String alti_master_url = (String) payload.get("alti_master_url");
            String alti_slave_url = (String) payload.getOrDefault("alti_slave_url",null);
            String alti_rescue_url = (String) payload.getOrDefault("alti_rescue_url",null);
            String alti_user = (String) payload.get("alti_user");
            String alti_password = (String) payload.get("alti_password");
            new VmFirstMakeDatePreProcess(job, dataSQL, env, alti_master_url, alti_slave_url, alti_rescue_url, alti_user, alti_password);
        }
    }

    void start();

    class EmptyPreProcess implements PreProcess {
        private static final Logger logger = LoggerFactory.getLogger(EmptyPreProcess.class);
        @Override
        public void start() {
            logger.info("Empty !!!!!!!!!!!!!!!!!!!!!!!!!!!");
        }
    }
}
