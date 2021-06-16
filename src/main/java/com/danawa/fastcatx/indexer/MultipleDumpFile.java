package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.ProcedureIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MultipleDumpFile {
    private static final Logger logger = LoggerFactory.getLogger(MultipleDumpFile.class);

    private final RestTemplate restTemplate = new RestTemplate(Utils.getRequestFactory());
    private Set<Integer> subStarted = Collections.synchronizedSet(new LinkedHashSet<>());
    private Set<Integer> startedProcedureGroupSeq = Collections.synchronizedSet(new LinkedHashSet<>());
    private Set<Integer> finishedGroupSeq = Collections.synchronizedSet(new LinkedHashSet<>());

    private IndexService service;

    public void index(Job job, String host, Integer port, String esUsername, String esPassword, String scheme, String index,
                      Boolean reset, String filterClassName,
                      Integer bulkSize, Integer threadSize, String pipeLine, Map<String, Object> indexSettings,
                      Map<String, Object> payload) {
        try {
            logger.info("params: {}", payload);
            // --------- 파라미터 변수 변환 시작 ---------

            //파일 경로.
            String path = (String) payload.get("path");
            // 파일 인코딩. utf-8, cp949 등..
            String encoding = (String) payload.get("encoding");
            // 테스트용도로 데이터 갯수를 제한하고 싶을때 수치.
            Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

            //프로시저 호출에 필요한 정보
            String driverClassName = (String) payload.get("driverClassName");
            String url = (String) payload.get("url");
            String user = (String) payload.get("user");
            String password = (String) payload.get("password");
            String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
            String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
            String rsyncPath = (String) payload.get("rsyncPath"); //rsync - Full Path
            String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
            String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync 전송속도 - 1024 = 1m/s
            boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // 프로시저 스킵 여부
            boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync 스킵 여부

//            모의 색인 실행 여부
            boolean dryRun = (Boolean) payload.getOrDefault("dryRun",false);
//            IDXP에서 subStart 호출없이 그룹시퀀스 색인 실행할지 여부
            boolean enableSelfStartSubStart = (Boolean) payload.getOrDefault("enableSelfStartSubStart",false);
//            오피스 전체 색인 실행 여부
            boolean enableOfficeIndexingJob = (Boolean) payload.getOrDefault("enableOfficeIndexingJob",false); //   office full index
//            Q인덱서 컨슘 on/off 처리 여부
            boolean enableOfficeAutoDynamic = (Boolean) payload.getOrDefault("enableOfficeAutoDynamic",false);
//            Q인덱서 컨슘 갯수
            int officeIndexConsumeCount = (int) payload.getOrDefault("officeIndexConsumeCount",1);

//            오피스 Q 이름
            String officeQueueName = (String) payload.getOrDefault("officeQueueName","");
//            오피스 전체색인 URL
            String officeFullIndexUrl = (String) payload.getOrDefault("officeFullIndexUrl","");
//            오피스 Q 인덱서 URL
            String officeQueueIndexUrl = (String) payload.getOrDefault("officeQueueIndexUrl","");
//            오피스 색인 체크 URL
            String officeCheckUrl = (String) payload.getOrDefault("officeCheckUrl","");
//            문자열로 나열된 그룹시퀀스 분리
            Set<Integer> groupSeqList = parseGroupSeq(String.valueOf(payload.get("groupSeq")));
            // --------- 파라미터 변수 변환 마지막 ---------

            if (groupSeqList.size() == 0) {
                // 그룹시퀀스가 이상하면 색인 중지
                logger.warn("Not Found GroupSeq.. example: `1,2,3,4-10`");
                return;
            }
            // GroupSeq 개별 색인 시작 호출을 자동으로 진행함.
            if (enableSelfStartSubStart) {
                // 시작할 그룹시퀀스
                job.getGroupSeq().addAll(groupSeqList);
                logger.info("enable self Sub Start GroupSeq >>> {} ", job.getGroupSeq());
            }

            if (dryRun) {
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
                logger.info("================== >>> DRY_RUN <<< ==============");
            }

            // IDXP를 사용안할때, 오피스 전채색인, 프로시저 시작 할때만 새로운 스래드 시작
            if (enableSelfStartSubStart && enableOfficeIndexingJob && !procedureSkip) {
                logger.info("start office thread");
//                logger.info("dryRun: {}", dryRun);
//                logger.info("enableAutoDynamic: {}", enableAutoDynamic);
//                logger.info("startedProcedureGroupSeq: {}", startedProcedureGroupSeq);
//                logger.info("officeFullIndexUrl: {}", officeFullIndexUrl);
//                logger.info("groupSeqList: {}", groupSeqList);
//                logger.info("officeQueueIndexUrl: {}", officeQueueIndexUrl);
//                logger.info("officeCheckUrl: {}", officeCheckUrl);
//                logger.info("officeIndexConsumeCount: {}", officeIndexConsumeCount);
//                logger.info("officeQueueName: {}", officeQueueName);
                // 오피스 색인 스래드 실행
                new Thread(new OfficeIndexingJob(
                        dryRun,
                        job,
                        String.valueOf(payload.get("groupSeq")),
                        enableOfficeAutoDynamic,
                        startedProcedureGroupSeq,
                        officeFullIndexUrl,
                        groupSeqList,
                        officeQueueIndexUrl,
                        officeCheckUrl,
                        officeIndexConsumeCount,
                        officeQueueName
                )).start();
            } else {
                logger.info("not start office trigger, enableSelfStartSubStart: {}, enableOfficeIndexingJob: {}, procedureSkip: {}", enableSelfStartSubStart, enableOfficeIndexingJob, !procedureSkip);
            }

            CountDownLatch latch = new CountDownLatch(groupSeqList.size());
            List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());

            long n = 0;
            while (groupSeqList.size() != job.getGroupSeq().size() || subStarted.size() != job.getGroupSeq().size()) {
                Iterator<Integer> iterator = job.getGroupSeq().iterator();

                if("STOP".equalsIgnoreCase(job.getStatus())) {
//                    1. 기존 인덱싱하던 쓰래드를 기다린다.
//                    2. 남은 그룹 시퀀스가 있으면 실행완료추가한다.
                    subStarted.addAll(groupSeqList);
                    logger.info("Stop Signal. Started GroupSeq: {}", job.getGroupSeq());
                    job.getGroupSeq().addAll(groupSeqList);
                    break;
                } else {
                    while (iterator.hasNext()) {
                        Integer groupSeq = iterator.next();
                        if (!subStarted.contains(groupSeq)) {
                            if (subStarted.size() == 0) {
//                            처음일때만 리셋가능.
                                service = new IndexService(host, port, scheme, esUsername, esPassword);
                                // 인덱스를 초기화하고 0건부터 색인이라면.
                                if (reset && !dryRun) {
                                    if (service.existsIndex(index)) {
                                        if(service.deleteIndex(index)) {
                                            service.createIndex(index, indexSettings);
                                        }
                                    }
                                }
                            }
                            subStarted.add(groupSeq);
                            logger.info("Started GroupSeq Indexing : {}", groupSeq);
//                        groupSeq 개별로 쓰래드 생성하여 색인진행.
                            new Thread(() -> {
                                try {
                                    long tst = System.currentTimeMillis();

//                                    덤프파일 경로
                                    String dumpFileDirPath = String.format("%sV%d", path, groupSeq);
                                    File dumpFileDir = new File(dumpFileDirPath);
                                    Ingester finalIngester = null;
                                    if (!dumpFileDir.exists()) {
                                        dumpFileDir.mkdirs();
                                    }
                                    logger.info("dumpFileDirPath: {}", dumpFileDirPath);

                                    //프로시져
                                    CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName, groupSeq, dumpFileDirPath);
                                    //RSNYC
                                    RsyncCopy rsyncCopy = new RsyncCopy(rsyncIp, rsyncPath, dumpFileDirPath, bwlimit, groupSeq);

                                    boolean execProdure = false;
                                    boolean rsyncStarted = false;
                                    //덤프파일 이름
                                    String dumpFileName = "prodExt_" + groupSeq;

                                    //SKIP 여부에 따라 프로시저 호출
                                    if(procedureSkip == false) {
                                        logger.info("procedure started. groupSeq: {}", groupSeq);
                                        long st = System.currentTimeMillis();
                                        if (!dryRun) {
                                            execProdure = procedure.callSearchProcedure();
                                        } else {
                                            // random max 2 min
                                            logger.info("[DRY_RUN] groupSeq: {} procedure skip. random sleep max 2 min", groupSeq);
                                            Thread.sleep((int) Math.abs( ((Math.random() * 99999) % 120) * 1000));
                                        }
                                        long nt = System.currentTimeMillis();
                                        logger.info("procedure finished. groupSeq: {}, elapsed time: {} s", groupSeq, (nt - st) / 1000);

                                    } else {
                                        logger.info("not start procedure. groupSeq: {}", groupSeq);
                                    }

//                                      그룹시퀀스를 추가할때마다 오피스 색인 작업 시작
                                    startedProcedureGroupSeq.add(groupSeq);

                                    if (!dryRun) {
                                        // Not DryRun !!!!!!!!!!!!!!!!!!
                                        //프로시저 결과 True, R 스킵X or 프로시저 스킵 and rsync 스킵X
                                        if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
                                            rsyncCopy.start();
                                            Thread.sleep(3000);
                                            rsyncStarted = rsyncCopy.copyAsync();
                                        }
                                        logger.info("rsyncStarted : {}" , rsyncStarted );
                                        int fileCount = 0;
                                        if(rsyncStarted || rsyncSkip) {
                                            logger.info("rsyncSkip : {}" , rsyncSkip);
                                            if(rsyncSkip) {
                                                long count = 0;
                                                if(Files.isDirectory(Paths.get(dumpFileDirPath))){
                                                    count = Files.walk(Paths.get(dumpFileDirPath)).filter(Files::isRegularFile).count();
                                                }else if(Files.isRegularFile(Paths.get(dumpFileDirPath))){
                                                    count = 1;
                                                }

                                                if(count == 0){
                                                    throw new FileNotFoundException("파일을 찾을 수 없습니다. (filepath: " + dumpFileDirPath + "/)");
                                                }
                                            } else {
                                                //파일이 있는지 1초마다 확인
                                                while (!Utils.checkFile(dumpFileDirPath, dumpFileName)) {
                                                    if (fileCount == 10) break;
                                                    Thread.sleep(1000);
                                                    fileCount++;
                                                    logger.info("{} 파일 확인 count: {} / 10", dumpFileName, fileCount);
                                                }

                                                if (fileCount == 10) {
                                                    throw new FileNotFoundException("rsync 된 파일을 찾을 수 없습니다. (filepath: " + dumpFileDirPath + "/" + dumpFileName + ")");
                                                }
                                            }
                                            //GroupSeq당 하나의 덤프파일이므로 경로+파일이름으로 인제스터 생성
                                            logger.info("file Path - Name  : {} - {}", dumpFileDirPath, dumpFileName);
                                            finalIngester = new ProcedureIngester(dumpFileDirPath, dumpFormat, encoding, 1000, limitSize);
                                        }

                                        Filter filter = (Filter) Utils.newInstance(filterClassName);

                                        IndexService indexService = new IndexService(host, port, scheme, esUsername, esPassword, groupSeq);
//                                        색인
                                        if (threadSize > 1) {
                                            indexService.indexParallel(finalIngester, index, bulkSize, filter, threadSize, job, pipeLine);
                                        } else {
                                            indexService.index(finalIngester, index, bulkSize, filter, job, pipeLine);
                                        }
                                    } else {
                                        // DryRun !!!!!!!!!!!!!!!!!!
                                        // random max 2 min
                                        int s = (int) Math.abs( ((Math.random() * 99999999) % 120) * 1000);
                                        Thread.sleep(s);
                                    }

                                    long tnt = System.currentTimeMillis();
                                    finishedGroupSeq.add(groupSeq);
                                    logger.info("Full Index Success. GroupSeq: {} elapsed: {}s thread Buy~!   finish groupSeq: {}", groupSeq, (tnt - tst) / 1000, finishedGroupSeq);
                                } catch (InterruptedException | FileNotFoundException e) {
                                    logger.error("", e);
                                    Thread.currentThread().interrupt();
                                    exceptions.add(e);
                                } catch (StopSignalException e) {
                                    logger.info("Stop Signal GroupSeq Wait: {}", groupSeq);
                                    logger.error("", e);
                                    exceptions.add(e);
                                } catch (Exception e) {
                                    logger.error("", e);
                                    exceptions.add(e);
                                } finally {
                                    latch.countDown();
                                }
                            }).start();

                        }
                    }
                }

                try {
                    // 1초 대기
                    Thread.sleep(1000);
                    n++;
                    if (n % 30 == 0) {
                        logger.info("Wait Indexing.. jobId: {}, started GroupSeq Count: {}, groupseq numbers: {}", job.getId(), subStarted.size(), subStarted);
                        n = 0;
                    }
                } catch (InterruptedException ignore){}
            }

            logger.info("GroupSeq All SubStart Started. jobId: {}, started GroupSeq Count: {}, groupseq numbers: {}", job.getId(), subStarted.size(), subStarted);

            // 최대 3일동안 기다려본다.
            if (!latch.await(3, TimeUnit.DAYS)) {
                job.setStopSignal(true);
            }
            logger.info("finished. jobId: {}", job.getId());
            if (exceptions.size() == 0) {
                job.setStatus(IndexJobRunner.STATUS.SUCCESS.name());
//                모든 스래드가 종료되고, 서버로 색인 완료를 체크
            } else {
                job.setStatus(IndexJobRunner.STATUS.STOP.name());
                job.setError(exceptions.toString());
            }
        } catch (Exception e) {
            logger.error("", e);
            job.setStatus(IndexJobRunner.STATUS.STOP.name());
            job.setError(e.getMessage());
        }
    }

//    그룹시퀀스 문자열 파싱
    public Set<Integer> parseGroupSeq(String groupSeq) {
        Set<Integer> list = new LinkedHashSet<>();
        if (groupSeq == null || "".equals(groupSeq)) {
            return list;
        }
        String[] arr1 = groupSeq.split(",");
        for (int i = 0; i < arr1.length; i++) {
            String[] r = arr1[i].split("-");
            if (r.length > 1) {
                list.addAll(getRange(Integer.parseInt(r[0]), Integer.parseInt(r[1])));
            } else {
                list.add(Integer.parseInt(r[0]));
            }
        }
        return list;
    }

    public Set<Integer> getRange(int from, int to) {
        Set<Integer> range = new LinkedHashSet<>();
        for (int i = from; i <= to; i++) {
            range.add(i);
        }
        return range;
    }

}
