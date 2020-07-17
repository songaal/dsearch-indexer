package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class IndexJobManager {
    private ConcurrentHashMap<UUID, Job> jobs = new ConcurrentHashMap<>();

    private enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }

    public Job stop(UUID id) {
        Job job = jobs.get(id);
        if (job != null && STATUS.RUNNING.equals(job.getStatus())) {
            job.setStopSignal(true);
        }
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
        if ("FULL_INDEX".equalsIgnoreCase(action)) {
            new Thread(new IndexJobRunner(job)).start();
        } else if ("DYNAMIC_INDEX".equalsIgnoreCase(action)) {
            new Thread(new DynamicIndexJobRunner(job)).start();
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
