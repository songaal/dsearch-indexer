package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexJobManager {
    private static Logger logger = LoggerFactory.getLogger(IndexJobManager.class);
    private ConcurrentHashMap<UUID, Job> jobs = new ConcurrentHashMap<>();

    public Job remove(UUID id) {
        Job job = jobs.get(id);
        if (job != null && !"RUNNING".equalsIgnoreCase(job.getStatus())) {
            jobs.remove(id);
        } else {
            job = null;
        }
        return job;
    }
    public List<UUID> getIds() {
        List<UUID> ids = new ArrayList<>();
        Iterator<UUID> iterator = jobs.keySet().iterator();
        while (iterator.hasNext()) {
            ids.add(iterator.next());
        }
        return ids;
    }

    public Job stop(UUID id) {
        Job job = jobs.get(id);
        if (job != null && "RUNNING".equalsIgnoreCase(job.getStatus())) {
            job.setStopSignal(true);
        }
        job.setStatus("STOP");
        return job;
    }

    public Job status(UUID id) {
        return jobs.get(id);
    }

    public Job start(String action, Map<String, Object> payload) {
        UUID id = genId();
        Job job = new Job();
        job.setId(id);
        job.setRequest(payload);
        job.setAction(action);
        jobs.put(id, job);
        logger.info("job ID: {}", id.toString());
        if ("FULL_INDEX".equalsIgnoreCase(action)) {
            new Thread(new IndexJobRunner(job)).start();
        }
        return job;
    }

    private UUID genId() {
        UUID id = UUID.randomUUID();
        while (true) {
            if (jobs.containsKey(id)) {
                id = UUID.randomUUID();
            } else {
                break;
            }
        }
        return id;
    }


}
