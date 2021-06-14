package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.*;
//import com.danawa.fastcatx.indexer.preProcess.VmFirstMakeDatePreProcess;
import com.danawa.fastcatx.indexer.preProcess.PreProcess;
import com.github.fracpete.processoutput4j.output.CollectingProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

public class IndexJobRunner implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(IndexJobRunner.class);
    public enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }
    private Job job;
    private IndexService service;
    private Ingester ingester;

    private final RestTemplate restTemplate = new RestTemplate(Utils.getRequestFactory());
    private final Gson gson = new Gson();

    private boolean autoDynamic;
    private String autoDynamicIndex;
    private List<String> autoDynamicQueueNames;
    private String autoDynamicCheckUrl;
    private String autoDynamicQueueIndexUrl;
    private int autoDynamicQueueIndexConsumeCount = 1;


    public IndexJobRunner(Job job) {
        this.job = job;
        job.setStatus(STATUS.READY.name());
        job.setStartTime(System.currentTimeMillis() / 1000);
        service = null;
        ingester = null;
    }

    public String getDirectoryAllFiles(String path) {
        StringBuffer sb = new StringBuffer();

        try {
            if(Files.isDirectory(Paths.get(path))){
                logger.info("{} is directory !", path);
                Files.walk(Paths.get(path))
                        .filter(Files::isRegularFile)
                        .forEach(item -> {
                            if(sb.length() == 0){
                                sb.append(item.toString());
                            }else{
                                sb.append("," + item.toString());
                            }
                        });
            }else{
                logger.info("{} is regularFile !", path);
                sb.append(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

    @Override
    public void run() {
        try {
            job.setStatus(STATUS.RUNNING.name());
            Map<String, Object> payload = job.getRequest();
            logger.debug("{}", gson.toJson(payload));
            Boolean preProcess = (Boolean) payload.getOrDefault("preProcess", false);
            if (preProcess) {
                PreProcess process = new PreProcess.EmptyPreProcess();
                process.starter(job);
                job.setStatus(STATUS.SUCCESS.name());
                return;
            }

            logger.info("Started Indexing Job Runner");
            // 공통
            // ES 호스트
            String host = (String) payload.get("host");
            // ES 포트
            Integer port = (Integer) payload.get("port");
            String esUsername = null; // ES 유저
            String esPassword = null; // ES 패스워드
            if (payload.get("esUsername") != null && payload.get("esPassword") != null) {
                esUsername = (String)payload.get("esUsername");
                esPassword = (String)payload.get("esPassword");
            }

            // http, https
            String scheme = (String) payload.get("scheme");
            // 색인이름.
            String index = (String) payload.get("index");
            // 소스타입: ndjson, csv, jdbc 등..
            String type = (String) payload.get("type");
            // index가 존재하면 색인전에 지우는지 여부. indexTemplate 이 없다면 맵핑과 셋팅은 모두 사라질수 있다.
            Boolean reset = (Boolean) payload.getOrDefault("reset", true);
            // 필터 클래스 이름. 패키지명까지 포함해야 한다. 예) com.danawa.fastcatx.filter.MockFilter
            String filterClassName = (String) payload.get("filterClass");
            // ES bulk API 사용시 벌크갯수.
            Integer bulkSize = (Integer) payload.get("bulkSize");
            Integer threadSize = (Integer) payload.getOrDefault("threadSize", 1);
            String pipeLine = (String) payload.getOrDefault("pipeLine","");

            Map<String, Object> indexSettings;
            try {
                indexSettings = (Map<String, Object>) payload.get("_indexingSettings");
                if (indexSettings == null) {
                    throw new ClassCastException();
                }
            } catch (ClassCastException e) {
                indexSettings = new HashMap<>();
            }
            logger.debug("index: {}", index);
            /**
             * file기반 인제스터 설정
             */
            //파일 경로.
            String path = (String) payload.get("path");
            // 파일 인코딩. utf-8, cp949 등..
            String encoding = (String) payload.get("encoding");
            // 테스트용도로 데이터 갯수를 제한하고 싶을때 수치.
            Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

            // 자동으로 동적색인 on/off
            autoDynamic = (Boolean) payload.getOrDefault("autoDynamic",false);
            if (autoDynamic) {
//                    자동으로 동적색인 필수 파라미터
                autoDynamicIndex = index;
                autoDynamicQueueNames = Arrays.asList(((String) payload.getOrDefault("autoDynamicQueueNames","")).split(","));
                autoDynamicCheckUrl = (String) payload.getOrDefault("autoDynamicCheckUrl","");
                autoDynamicQueueIndexUrl = (String) payload.getOrDefault("autoDynamicQueueIndexUrl","");
                try {
                    autoDynamicQueueIndexConsumeCount = (int) payload.getOrDefault("autoDynamicQueueIndexConsumeCount",1);
                } catch (Exception ignore) {
                    autoDynamicQueueIndexConsumeCount = Integer.parseInt((String) payload.getOrDefault("autoDynamicQueueIndexConsumeCount","1"));
                }
                // 큐 이름이 여러개 일 경우.
                for (String autoDynamicQueueName : autoDynamicQueueNames) {
                    try {
                        // 동적색인 off
                        updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, 0);
                        Thread.sleep(1000);
                    } catch (Exception e){
                        logger.error("", e);
                    }
                }
                logger.info("[{}] autoDynamic >>> Close <<<", autoDynamicIndex);
            }

            boolean dryRun = (Boolean) payload.getOrDefault("dryRun",false); // rsync 스킵 여부
            if (dryRun) {
                int r = (int) Math.abs((Math.random() * 999999) % 120) * 1000;
                Thread.sleep(r);
                job.setStatus(STATUS.SUCCESS.name());
                logger.info("[DRY_RUN] index: {} procedure Type {}. Index Success", index, type);
                return;
            }

            if (type.equals("ndjson")) {
                ingester = new NDJsonIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("csv")) {
                ingester = new CSVIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("file")) {
                String headerText = (String) payload.get("headerText");
                String delimiter = (String) payload.get("delimiter");
                ingester = new DelimiterFileIngester(path, encoding, 1000, limitSize, headerText,delimiter);
            } else if(type.equals("konan")){
                ingester = new KonanIngester(path, encoding, 1000, limitSize);
            }else if (type.equals("jdbc")) {
                String dataSQL = (String) payload.get("dataSQL");
                Integer fetchSize = (Integer) payload.get("fetchSize");
                Integer maxRows = (Integer) payload.getOrDefault("maxRows", 0);
                Boolean useBlobFile = (Boolean) payload.getOrDefault("useBlobFile", false);
                if (payload.get("_jdbc") != null) {
                    Map<String, Object> jdbcMap = (Map<String, Object>) payload.get("_jdbc");

                    int sqlCount = 2;
                    ArrayList<String> sqlList = new ArrayList<String>();

                    String driverClassName = (String) jdbcMap.get("driverClassName");
                    String url = (String) jdbcMap.get("url");
                    String user = (String) jdbcMap.get("user");
                    String password = (String) jdbcMap.get("password");

                    sqlList.add(dataSQL);
                    //dataSQL2,3.... 있을 경우
                    while ( payload.get("dataSQL" + String.valueOf(sqlCount)) != null ) {
                        sqlList.add((String) payload.get("dataSQL" + String.valueOf(sqlCount)));
                        sqlCount++;
                    }

                    ingester = new JDBCIngester(driverClassName, url, user, password, bulkSize, fetchSize, maxRows, useBlobFile, sqlList);
                } else {
                    throw new IllegalArgumentException("jdbc argument");
                }
            } else if (type.equals("multipleDumpFile")) {
                // 다중 색인
                new MultipleDumpFile().index(job, host, port, esUsername, esPassword, scheme, index, reset, filterClassName, bulkSize, threadSize, pipeLine, indexSettings, payload);
                return;
            } else if (type.equals("procedure")) {

                //프로시저 호출에 필요한 정보
                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
                Integer groupSeq = null;
                if (payload.get("groupSeq") != null) {
                    try {
                        groupSeq = (Integer) payload.get("groupSeq");
                    } catch (Exception e) {
                        groupSeq = Integer.parseInt((String) payload.get("groupSeq"));
                    }
                }
                String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
                String rsyncPath = (String) payload.get("rsyncPath"); //rsync - Full Path
                String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
                String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync 전송속도 - 1024 = 1m/s
                boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // 프로시저 스킵 여부
                boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync 스킵 여부

                //프로시져
                CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName,groupSeq,path);
                //RSNYC
                RsyncCopy rsyncCopy = new RsyncCopy(rsyncIp,rsyncPath,path,bwlimit,groupSeq);

                boolean execProdure = false;
                boolean rsyncStarted = false;
                //덤프파일 이름
                String dumpFileName = "prodExt_"+groupSeq;

                //SKIP 여부에 따라 프로시저 호출
                if(procedureSkip == false) {
                    execProdure = procedure.callSearchProcedure();
                }
//                logger.info("execProdure : {}",execProdure);

                //프로시저 결과 True, R 스킵X or 프로시저 스킵 and rsync 스킵X
                if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
                    rsyncCopy.start();
                    Thread.sleep(3000);
                    rsyncStarted = rsyncCopy.copyAsync();
                }
                logger.info("rsyncStarted : {}" , rsyncStarted );
                int fileCount = 0;
                if(rsyncStarted || rsyncSkip) {

                    if(rsyncSkip) {
                        logger.info("rsyncSkip : {}" , rsyncSkip);

                        long count = 0;
                        if(Files.isDirectory(Paths.get(path))){
                            count = Files.walk(Paths.get(path)).filter(Files::isRegularFile).count();
                        }else if(Files.isRegularFile(Paths.get(path))){
                            count = 1;
                        }

                        if(count == 0){
                            throw new FileNotFoundException("파일을 찾을 수 없습니다. (filepath: " + path + "/)");
                        }
                    } else {
                        //파일이 있는지 1초마다 확인
                        while (!Utils.checkFile(path, dumpFileName)) {
                            if (fileCount == 10) break;
                            Thread.sleep(1000);
                            fileCount++;
                            logger.info("{} 파일 확인 count: {} / 10", dumpFileName, fileCount);
                        }

                        if (fileCount == 10) {
                            throw new FileNotFoundException("rsync 된 파일을 찾을 수 없습니다. (filepath: " + path + "/" + dumpFileName + ")");
                        }
                    }
                    //GroupSeq당 하나의 덤프파일이므로 경로+파일이름으로 인제스터 생성
//                    path += "/"+dumpFileName;
                    logger.info("file Path - Name  : {} - {}", path, dumpFileName);
                    ingester = new ProcedureIngester(path, dumpFormat, encoding, 1000, limitSize);
                }
            }
            else if (type.equals("procedure-link")) {

                //프로시저 호출에 필요한 정보
                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
                String groupSeqs = (String) payload.get("groupSeqs");
                String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
                String rsyncPath = (String) payload.get("rsyncPath"); //rsync - Full Path
                String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
                String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync 전송속도 - 1024 = 1m/s
                boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // 프로시저 스킵 여부
                boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync 스킵 여부
                String procedureThreads = (String) payload.getOrDefault("procedureThreads","4"); // rsync 스킵 여부

                String[] groupSeqLists = groupSeqs.split(",");

                // 1. groupSeqLists 수 만큼 프로시저 호출
                // 2. 체크
                // 3. 다 끝나면 rsync
                StringBuffer sb = new StringBuffer();
                Map<String, Boolean> procedureMap = new HashMap<>();

                if(procedureSkip == false) {
                    logger.info("Call Procedure");
                    ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(procedureThreads));

                    List threadsResults = new ArrayList<Future<Object>>();
                    for(String groupSeq : groupSeqLists){
                        Integer groupSeqNumber = Integer.parseInt(groupSeq);
                        Callable callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                String path = (String) payload.get("path");
                                logger.info("driverClassName: {}, url: {}, user: {}, password: {}, procedureName: {}, groupSeqNumber: {}, path: {}", driverClassName, url, user, password, procedureName, groupSeqNumber, path);
                                CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName, groupSeqNumber, path, true);
                                Map<String, Object> result = new HashMap<>();
                                result.put("groupSeq", groupSeq);
                                result.put("result", procedure.callSearchProcedure());
                                return result;
                            }
                        };

                        Future<Object> future = threadPool.submit(callable);
                        threadsResults.add(future);
                    }

                    for (Object f : threadsResults) {
                        Future<Object> future = (Future<Object>) f;
                        Map<String, Object> execProdure = (Map<String, Object>) future.get();
                        procedureMap.put((String) execProdure.get("groupSeq"), (Boolean) execProdure.get("result"));
                        logger.info("{} execProdure: {}", (String) execProdure.get("groupSeq"), (Boolean) execProdure.get("result"));
                    }

                    threadPool.shutdown();
                }

//                프로시저 -> multiThread
//                rsync ->  singleThread -> 1개 씩
                if(rsyncSkip == false){
                    ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(procedureThreads));
                    List threadsResults2 = new ArrayList<Future<Object>>();

                    // 폴더 생성
                    logger.info("path={}", path);
                    File file = new File(path);
                    if(!file.exists()){
                        boolean result = file.mkdir();
                        if(result){
                            logger.info("{} mkdir!!", path);
                        }else{
                            logger.info("{} mkdir failed..", path);
                        }
                    }

                    for(String groupSeq : groupSeqLists){
                        logger.info("groupSeq : {}", groupSeq);
                        Integer groupSeqNumber = Integer.parseInt(groupSeq);

                        Callable callable = new Callable() {
                            @Override
                            public Object call() throws Exception {
                                String path = (String) payload.get("path");
                                String dumpFileName = "linkExt_" + groupSeq;

                                //덤프파일 이름
                                File file = new File(path + "/" + dumpFileName);
                                logger.info("fila path: {}", path + "/" + dumpFileName);

                                if (file.exists()) {
                                    logger.info("기존 파일 삭제 : {}", file);
                                    file.delete();
                                }else{
                                    logger.info("기존 파일을 찾을수 없음: {}", file);
                                }

                                boolean execProdure = false;
                                boolean rsyncStarted = false;
                                //SKIP 여부에 따라 프로시저 호출
                                if(procedureSkip == false) {
                                    execProdure = procedureMap.get(groupSeq);
                                }
//                                logger.info("execProdure : {}",execProdure);
                                RSync rsync = new RSync()
                                        .source(rsyncIp+"::" + rsyncPath+"/linkExt_"+groupSeqNumber)
                                        .destination(path)
                                        .recursive(true)
                                        .archive(true)
                                        .compress(true)
                                        .bwlimit(bwlimit)
                                        .inplace(true);

                                //프로시저 결과 True, R 스킵X or 프로시저 스킵 and rsync 스킵X
                                if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
                                    CollectingProcessOutput output = rsync.execute();
                                    if( output.getExitCode() > 0){
                                        logger.error("{}", output.getStdErr());
                                    }
                                }
                                logger.info("rsyncStarted : {}" , rsyncStarted );

                                if(rsyncStarted || rsyncSkip) {
                                    if(rsyncSkip) {
                                        logger.info("rsyncSkip : {}" , rsyncSkip);
                                    }
                                }
                                String filepath = path + "/" + dumpFileName;

                                if(!rsyncSkip) {
                                    //파일이 있는지 1초마다 확인
                                    int fileCount = 0;
                                    while(!Utils.checkFile(path, dumpFileName)){
                                        if(fileCount == 10) break; // 무한루프 회피
                                        Thread.sleep(1000);
                                        fileCount++;
                                        logger.info("{} 파일 확인 count: {} / 10", dumpFileName, fileCount);
                                    }

                                    if(fileCount == 10){
                                        throw new FileNotFoundException("rsync 된 파일을 찾을 수 없습니다. (filepath: "+path + "/" + dumpFileName + ")");
                                    }
                                }
                                return filepath;
                            }
                        };
                        Future<Object> future = threadPool.submit(callable);
                        threadsResults2.add(future);
                    }

                    for (Object f : threadsResults2) {
                        Future<Object> future = (Future<Object>) f;
                        String filepath = (String) future.get();
                        logger.info("filepath: {}", filepath);
                        sb.append(filepath + ",");
                    }

                    threadPool.shutdown();
                }
                else{
                    sb.append(path);
//                    for(String groupSeq : groupSeqLists){
//                        String filepath = (String) payload.get("path");
//                        String dumpFileName = "linkExt_"+groupSeq;
//                        sb.append(filepath + "/" + dumpFileName + ",");
//                    }
                }

                if(sb.length() > 0 && sb.charAt(sb.length()-1) == ','){
                    sb.deleteCharAt(sb.length()-1);
                }

                ingester = new ProcedureLinkIngester(sb.toString(), dumpFormat, encoding, 1000, limitSize);
            }

            Ingester finalIngester = ingester;
            Filter filter = (Filter) Utils.newInstance(filterClassName);

            if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                logger.info("[STOP SIGNAL] type: procedure");
                throw new StopSignalException();
            }
//            service = new IndexService(host, port, scheme);
            service = new IndexService(host, port, scheme, esUsername, esPassword);
            // 인덱스를 초기화하고 0건부터 색인이라면.
            if (reset) {
                if (service.existsIndex(index)) {
                    if(service.deleteIndex(index)) {
                        service.createIndex(index, indexSettings);
                    }
                }
            }

            if (threadSize > 1) {
                service.indexParallel(finalIngester, index, bulkSize, filter, threadSize, job, pipeLine);
            } else {
                service.index(finalIngester, index, bulkSize, filter, job, pipeLine);
            }

            job.setStatus(STATUS.SUCCESS.name());
        } catch (StopSignalException e) {
            job.setStatus(STATUS.STOP.name());
        }catch (FileNotFoundException e){
            job.setStatus(STATUS.STOP.name());
            job.setError(e.getMessage());
            logger.error("error .... ", e);
        } catch (Exception e) {
            job.setStatus(STATUS.ERROR.name());
            job.setError(e.getMessage());
            logger.error("error .... ", e);
        } finally {
            job.setEndTime(System.currentTimeMillis() / 1000);
            if (autoDynamic) {
                new Thread(() -> {
                    int r = 20;
                    logger.info("create success check thread");
                    while (true) {
                        try {
//                            색인 취소 체크. 동적색인 on
                            if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                                logger.info("STOP SIGNAL");
                                for (String autoDynamicQueueName : autoDynamicQueueNames) {
                                    try {
                                        updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, autoDynamicQueueIndexConsumeCount);
                                        Thread.sleep(1000);
                                    } catch (Exception e){
                                        logger.error("", e);
                                    }
                                }
                                break;
                            }
//                            색인 완료 여부 체크
                            logger.info("상테 체크 URL: {}", autoDynamicCheckUrl);
                            ResponseEntity<String> searchCheckResponse = restTemplate.exchange(autoDynamicCheckUrl,
                                    HttpMethod.GET,
                                    new HttpEntity(new HashMap<String, Object>()),
                                    String.class
                            );
                            String status = null;
                            try {
                                Map<String, Object> body = gson.fromJson(searchCheckResponse.getBody(), Map.class);
                                Map<String, Object> info = gson.fromJson(gson.toJson(body.get("info")), Map.class);
                                status = String.valueOf(info.get("status"));
                            }catch (Exception ignore) {}
//                            색인이 완료라면 동적색인 ON
                            if ("SUCCESS".equalsIgnoreCase(status) || "NOT_STARTED".equalsIgnoreCase(status)) {
                                for (String autoDynamicQueueName : autoDynamicQueueNames) {
                                    try {
                                        updateQueueIndexerConsume(false, autoDynamicQueueIndexUrl, autoDynamicQueueName, autoDynamicQueueIndexConsumeCount);
                                        Thread.sleep(1000);
                                    } catch (Exception e){
                                        logger.error("", e);
                                    }
                                }
                                logger.info("[{}] autoDynamic >>> Open <<<", autoDynamicIndex);
                                break;
                            }
                            Thread.sleep(60 * 1000);
                        } catch (Exception e) {
                            logger.error("", e);
                            r --;
                            if (r == 0) {
                                logger.warn("max retry!!!!");
                                break;
                            } else {
                                try {
                                    Thread.sleep(1000);
                                } catch (InterruptedException ignore) {}
                            }
                        }
                    }
                    logger.info("success check Thread terminate");
                }).start();
            }
        }
    }

    public void updateQueueIndexerConsume(boolean dryRun, String queueIndexerUrl, String queueName, int consumeCount) {
        Map<String, Object> body = new HashMap<>();
        body.put("queue", queueName);
        body.put("size", consumeCount);
        logger.info("QueueIndexUrl: {}, queue: {}, count: {}", queueIndexerUrl, queueName, consumeCount);
        if (!dryRun) {
            ResponseEntity<String> response = restTemplate.exchange(queueIndexerUrl,
                    HttpMethod.PUT,
                    new HttpEntity(body),
                    String.class
            );
            logger.info("edit Consume Response: {}", response);
        } else {
            logger.info("[DRY_RUN] queue indexer request skip");
        }
    }

}
