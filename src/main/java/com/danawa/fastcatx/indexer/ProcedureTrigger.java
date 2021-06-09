package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;

public class ProcedureTrigger implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ProcedureTrigger.class);
    private boolean dryRun;
    private Job job;
    private Set<Integer> groupSeqList;

    private boolean enableAutoDynamic;
    private Set<Integer> startedProcedureGroupSeq;
    private String officeQueueIndexUrl;
    private String startOfficeFullIndexPrefixUrl;
    private String officeCheckUrlPrefix;
    private int queueIndexConsumeCount;
    private String officeQueueName;

    private Set<Integer> startedFullIndexGroupSeq = new HashSet<>();
    private final Gson gson = new Gson();
    private final RestTemplate restTemplate = new RestTemplate(Utils.getRequestFactory());

    int maxRetry = 10;
    private long wait = 0;

    boolean loop = true;

    int checkIgnoreCount = 5;

    private Set<Integer> endGroupSeq = new HashSet<>();

    public ProcedureTrigger(boolean dryRun, Job job, boolean enableAutoDynamic, Set<Integer> startedProcedureGroupSeq, String startOfficeFullIndexPrefixUrl, Set<Integer> groupSeqList, String officeQueueIndexUrl, String officeCheckUrlPrefix, int queueIndexConsumeCount, String officeQueueName) {
        this.dryRun = dryRun;
        this.job = job;
        this.startedProcedureGroupSeq = startedProcedureGroupSeq;
        this.enableAutoDynamic = enableAutoDynamic;
        this.groupSeqList = groupSeqList;
        this.startOfficeFullIndexPrefixUrl = startOfficeFullIndexPrefixUrl;
        this.officeQueueIndexUrl = officeQueueIndexUrl;
        this.officeCheckUrlPrefix = officeCheckUrlPrefix;
        this.queueIndexConsumeCount = queueIndexConsumeCount;
        this.officeQueueName = officeQueueName;
    }

    @Override
    public void run() {
        while (loop) {
            try {
                if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                    logger.error("STOP SIGNAL !!!!!");
                    throw new StopSignalException();
                }
                int maxStartRetry = 10;
                int maxCheckRetry = 10;
                int maxOpenRetry = 10;

                // office collection full index start!!
                for (int groupSeq : groupSeqList) {
                    if (!startedFullIndexGroupSeq.contains(groupSeq) && startedProcedureGroupSeq.contains(groupSeq)) {
                        String startUrl = startOfficeFullIndexPrefixUrl;
                        if (startUrl != null && !"".equals(startUrl)) {
                            if (!startUrl.contains("?")) {
                                startUrl += "?";
                            }
                            startUrl += "&action=all";
                            startUrl += "&groupSeq=" + groupSeq;
                            startUrl += "&collectionName=s-prod-v" + groupSeq;
                            logger.info("Office Start FullIndex >> groupSeq: {}, startUrl: {}",groupSeq, startUrl);
                            for (;maxStartRetry >= 0; maxStartRetry--) {
                                try {
                                    ResponseEntity<String> startFullIndexResponse = restTemplate.exchange(startUrl,
                                            HttpMethod.GET,
                                            new HttpEntity(new HashMap<>()),
                                            String.class
                                    );
                                    startedFullIndexGroupSeq.add(groupSeq);
//                                    logger.info("Started Office FullIndex. GroupSeq: {}, URL: {},  Response: {}", groupSeq, startUrl, startFullIndexResponse);
                                    break;
                                } catch (Exception e) {
                                    logger.error("", e);
                                    Thread.sleep(1000);
                                }
                            }
                        }
                    }
                }

                // office check url
                if (checkIgnoreCount > 0) {
                    checkIgnoreCount --;
                } else {
                    for (int groupSeq : groupSeqList) {
//                        if (endGroupSeq.contains(groupSeq)) {
//                            continue;
//                        }
                        String officeCheckUrl = officeCheckUrlPrefix.contains("?") ? officeCheckUrlPrefix : officeCheckUrlPrefix + "?";
                        officeCheckUrl += "&collectionName=s-prod-v" + groupSeq;
                        officeCheckUrl += "&groupSeq=" + groupSeq;
                        for (; maxCheckRetry >= 0; maxCheckRetry--) {
                            try {
                                String status = "";
                                String tmpResponse = "";
                                ResponseEntity<String> officeCheckResponse = restTemplate.exchange(officeCheckUrl,
                                        HttpMethod.GET,
                                        new HttpEntity(new HashMap<>()),
                                        String.class
                                );
                                try {
                                    tmpResponse = officeCheckResponse.getBody();
                                    Map<String, Object> body = gson.fromJson(tmpResponse, Map.class);
                                    Map<String, Object> info = gson.fromJson(gson.toJson(body.get("info")), Map.class);
                                    status = String.valueOf(info.get("status"));
                                }catch (Exception ignore) {}
                                logger.info("Checking... status: {}, CheckUrl: {}", status, officeCheckUrl);
                                if ("SUCCESS".equalsIgnoreCase(status) || "NOT_STARTED".equalsIgnoreCase(status)) {
                                    endGroupSeq.add(groupSeq);
                                    logger.info("State Check Success");
                                } else if ("STOP".equalsIgnoreCase(status)) {
                                    job.setStopSignal(true);
                                    logger.info("State Check STOP!!!!!");
                                } else if ("RUNNING".equalsIgnoreCase(status)) {
//                                    logger.debug("running Url: {} body: {}", officeCheckUrl, tmpResponse);
                                }
                                break;
                            } catch (Exception e) {
                                logger.error("", e);
                                Thread.sleep(1000);
                            }
                        }
                    }


                }


                // all check finish!!!!
                if (groupSeqList.size() == endGroupSeq.size() && groupSeqList.size() == startedProcedureGroupSeq.size()) {
                    if (enableAutoDynamic) {
                        for (;maxOpenRetry >= 0; maxOpenRetry--) {
                            try {
                                Map<String, Object> body = new HashMap<>();
                                body.put("queue", officeQueueName);
                                body.put("size", queueIndexConsumeCount);
                                ResponseEntity<String> officeOpenResponse = null;
                                if (!dryRun) {
                                    officeOpenResponse = restTemplate.exchange(officeQueueIndexUrl,
                                            HttpMethod.PUT,
                                            new HttpEntity(body),
                                            String.class
                                    );
                                } else {
                                    logger.info("[DRY_RUN] >> Office << queue indexer request skip");
                                }
                                logger.info("office >>> OPEN <<< URL : {}", officeQueueIndexUrl);
                                logger.info("office dynamic Response : {}", officeOpenResponse);
                                break;
                            } catch (Exception e) {
                                logger.warn("officeQueueIndexUrl: {}", officeQueueIndexUrl);
                                logger.error("", e);
                                Thread.sleep(1000);
                            }
                        }
                    }
                    loop = false;
                }
                logger.info("Office Collections FULL_INDEX Waiting... startedProcedureGroupSeq: {} full index finish groupSeq: {}", startedProcedureGroupSeq, endGroupSeq);
                Thread.sleep(30 * 1000);
            } catch (StopSignalException se) {
                logger.warn("[[Manual Cancel]] procedure trigger");
                logger.error("cancel", se);
                loop = false;
                for (int groupSeq : groupSeqList) {
                    try {
                        String stopUrl = startOfficeFullIndexPrefixUrl;
                        if (stopUrl != null && !"".equals(stopUrl)) {
                            if (!stopUrl.contains("?")) {
                                stopUrl += "?";
                            }
                            String stopIndexUrl = String.format("%s&collectionName=s-prod-v%d&groupSeq=%d&action=stop_propagation", stopUrl, groupSeq, groupSeq);
                            String stopPropagationUrl = String.format("%s&collectionName=s-prod-v%d&groupSeq=%d&action=stop_indexing", stopUrl, groupSeq, groupSeq);
                            logger.info("STOP INDEX URL: {}", stopIndexUrl);
                            logger.info("STOP PROPAGATE URL: {}", stopPropagationUrl);
                            try {
                                restTemplate.exchange(stopIndexUrl, HttpMethod.GET, new HttpEntity(new HashMap<>()), String.class);
                            } catch (Exception ignore) {}
                            try {
                                restTemplate.exchange(stopPropagationUrl, HttpMethod.GET, new HttpEntity(new HashMap<>()), String.class);
                            } catch (Exception ignore) {}
                        }
                    }catch (Exception e) {
                        logger.error("", e);
                    }
                }
                break;
            } catch (Exception e) {
                logger.error("", e);
                logger.error("retry Count: {}", maxRetry);
                maxRetry --;
                if (maxRetry <= 0) {
                    // error request
                    loop = false;
                    break;
                }
            }
        }
        logger.info("office thread terminate");
    }
}
