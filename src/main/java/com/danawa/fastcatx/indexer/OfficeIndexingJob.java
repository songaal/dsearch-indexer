package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.*;

public class OfficeIndexingJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(OfficeIndexingJob.class);
    private boolean dryRun;
    private Job job;
    private Set<Integer> groupSeqList;

    private boolean enableAutoDynamic;
    private Set<Integer> finishedProcedureGroupSeq;
    private String officeQueueIndexUrl;
    private String officeFullIndexUrl;
    private String officeCheckUrl;
    private int queueIndexConsumeCount;
    private String officeQueueName;

    private Set<Integer> startedFullIndexGroupSeq = new HashSet<>();
    private final Gson gson = new Gson();
    private final RestTemplate restTemplate = new RestTemplate(Utils.getRequestFactory());

    int maxRetry = 10;
    private long wait = 0;

    boolean loop = true;

    private Set<Integer> endGroupSeq = new HashSet<>();

    public OfficeIndexingJob(boolean dryRun, Job job, String groupSeqStr, boolean enableAutoDynamic, Set<Integer> finishedProcedureGroupSeq, String officeFullIndexUrl, Set<Integer> groupSeqList, String officeQueueIndexUrl, String officeCheckUrl, int queueIndexConsumeCount, String officeQueueName) {
        logger.info("enableAutoDynamic: {}, office params: {}", enableAutoDynamic, job.getRequest());
        this.dryRun = dryRun;
        this.job = job;
        this.finishedProcedureGroupSeq = finishedProcedureGroupSeq;
        this.enableAutoDynamic = enableAutoDynamic;
        this.groupSeqList = groupSeqList;
        this.officeFullIndexUrl = officeFullIndexUrl;
        this.officeQueueIndexUrl = officeQueueIndexUrl;
        this.officeCheckUrl = officeCheckUrl;
        this.queueIndexConsumeCount = queueIndexConsumeCount;
        this.officeQueueName = officeQueueName;

        // 오피스 동적색인 처리
        if (!dryRun) {
            updateQueueIndexerConsume(10, 0);
            logger.info("OFFICE Dynamic >>> OFF <<<");
        } else {
            logger.info("[DRY_RUN] OFFICE Dynamic >>> Close <<<");
        }

        // 오피스 색인 호출
        String startUrl = officeFullIndexUrl;
        if (startUrl != null && !"".equals(startUrl)) {
            startUrl += "&action=all";
            startUrl += "&groupSeq=" + groupSeqStr;
            logger.info("오피스 전체 색인. {}", startUrl);
            for (int i = 0; i < 10; i++) {
                try {
                    restTemplate.exchange(startUrl, HttpMethod.GET, new HttpEntity(new HashMap<>()), String.class);
                    break;
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        }
    }

    @Override
    public void run() {
        int maxStartRetry = 10;
        int maxCheckRetry = 10;
        int maxOpenRetry = 10;
        while (loop) {
            try {
                // 색인 취소
                if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                    logger.error("STOP SIGNAL !!!!!");
                    throw new StopSignalException();
                }
                // 그룹시퀀스 반복하며, 시작 호출
                for (int groupSeq : groupSeqList) {
                    if (!startedFullIndexGroupSeq.contains(groupSeq) && finishedProcedureGroupSeq.contains(groupSeq)) {
                        String startUrl = officeFullIndexUrl;
                        // 호출
                        if (startUrl != null && !"".equals(startUrl)) {
                            startUrl += "&action=sub_start";
                            startUrl += "&groupSeq=" + groupSeq;
                            logger.info("서브 시작 호출 >> groupSeq: {}, startUrl: {}",groupSeq, startUrl);
                            for (;maxStartRetry >= 0; maxStartRetry--) {
                                try {
                                    restTemplate.exchange(startUrl,
                                            HttpMethod.GET,
                                            new HttpEntity(new HashMap<>()),
                                            String.class
                                    );
                                    startedFullIndexGroupSeq.add(groupSeq);
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
                boolean isEnd = false;
                if (startedFullIndexGroupSeq.size() == groupSeqList.size()) {
                    for (; maxCheckRetry >= 0; maxCheckRetry--) {
                        try {
                            String status = "";
                            ResponseEntity<String> officeCheckResponse = restTemplate.exchange(officeCheckUrl,
                                    HttpMethod.GET,
                                    new HttpEntity(new HashMap<>()),
                                    String.class
                            );
                            try {
                                String tmpResponse = officeCheckResponse.getBody();
                                Map<String, Object> body = gson.fromJson(tmpResponse, Map.class);
                                Map<String, Object> info = gson.fromJson(gson.toJson(body.get("info")), Map.class);
                                status = String.valueOf(info.get("status"));
                            }catch (Exception ignore) {}
                            logger.info("Checking... status: {}, CheckUrl: {}", status, officeCheckUrl);
                            if ("SUCCESS".equalsIgnoreCase(status) || "NOT_STARTED".equalsIgnoreCase(status)) {
                                isEnd = true;
                            } else if ("STOP".equalsIgnoreCase(status)) {
                                job.setStopSignal(true);
                            }
                            break;
                        } catch (Exception e) {
                            logger.error("", e);
                            Thread.sleep(1000);
                        }
                    }
                }

                // 색인 완료 처리
                if (isEnd && groupSeqList.size() == startedFullIndexGroupSeq.size()) {
                    loop = false;
                    updateQueueIndexerConsume(maxOpenRetry, queueIndexConsumeCount);
                    logger.info("오피스 색인 완료");
                } else {
                    logger.info("오피스 전체 색인 완료 대기. subStart: {}", startedFullIndexGroupSeq);
                    Thread.sleep(30 * 1000);
                }
            } catch (StopSignalException se) {
                logger.warn("[[Manual Cancel]] procedure trigger");
                logger.error("cancel", se);
                loop = false;
                try {
                    String stopUrl = officeFullIndexUrl;
                    if (stopUrl != null && !"".equals(stopUrl)) {
                        String stopIndexUrl = officeFullIndexUrl + "&action=stop_indexing";
                        logger.info("오피스 취소 요청 URL: {}", stopIndexUrl);
                        try {
                            restTemplate.exchange(stopIndexUrl, HttpMethod.GET, new HttpEntity(new HashMap<>()), String.class);
                        } catch (Exception ignore) {}
                    }
                }catch (Exception e) {
                    logger.error("", e);
                }
                updateQueueIndexerConsume(maxOpenRetry, queueIndexConsumeCount);
                break;
            } catch (Exception e) {
                logger.error("", e);
                logger.error("retry Count: {}", maxRetry);
                maxRetry --;
                if (maxRetry <= 0) {
                    loop = false;
                    break;
                }
            }
        }
        logger.info("office thread terminate");
    }

    public void updateQueueIndexerConsume(int maxOpenRetry, int consumeCount) {
        if (enableAutoDynamic) {
            logger.info("동적색인 Off 호출");
            for (;maxOpenRetry >= 0; maxOpenRetry--) {
                try {
                    Map<String, Object> body = new HashMap<>();
                    body.put("queue", officeQueueName);
                    body.put("size", consumeCount);
                    logger.info("QueueIndexUrl: {}, queue: {}, count: {}", officeQueueIndexUrl, officeQueueName, consumeCount);
                    ResponseEntity<String> response = restTemplate.exchange(officeQueueIndexUrl,
                            HttpMethod.PUT,
                            new HttpEntity(body),
                            String.class
                    );
                    logger.info("edit Consume Response: {}", response);
                    break;
                } catch (Exception e) {
                    logger.warn("officeQueueIndexUrl: {}", officeQueueIndexUrl);
                    logger.error("", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignore) {}
                }
            }
        }
    }

}
