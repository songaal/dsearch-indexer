package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IndexJobRunner implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(IndexJobRunner.class);
    private enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }
    private Job job;
    private IndexService service;
    private Ingester ingester;

    public IndexJobRunner(Job job) {
        this.job = job;
        job.setStatus(STATUS.READY.name());
        job.setStartTime(System.currentTimeMillis() / 1000);
        service = null;
        ingester = null;
    }

    @Override
    public void run() {
        try {
            job.setStatus(STATUS.RUNNING.name());
            Map<String, Object> payload = job.getRequest();
            logger.info("Started Indexing Job Runner");
            // 공통
            // ES 호스트
            String host = (String) payload.get("host");
            // ES 포트
            Integer port = (Integer) payload.get("port");
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

            if (type.equals("ndjson")) {
                ingester = new NDJsonIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("csv")) {
                ingester = new CSVIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("file")) {
                String headerText = (String) payload.get("headerText");
                String delimiter = (String) payload.get("delimiter");
                ingester = new DelimiterFileIngester(path, encoding, 1000, limitSize, headerText,delimiter);
            } else if (type.equals("jdbc")) {
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
            } else if (type.equals("procedure")) {

                //프로시저 호출에 필요한 정보
                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
                Integer groupSeq = (Integer) payload.get("groupSeq");
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
                logger.info("execProdure : {}",execProdure);

                //프로시저 결과 True, R 스킵X or 프로시저 스킵 and rsync 스킵X
                if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
                    rsyncCopy.start();
                    Thread.sleep(3000);
                    rsyncStarted = rsyncCopy.copyAsync();
                }
                logger.info("rsyncStarted : {}" , rsyncStarted );

                if(rsyncStarted || rsyncSkip) {

                    if(rsyncSkip) {
                        logger.info("rsyncSkip : {}" , rsyncSkip);
                    }
                    //GroupSeq당 하나의 덤프파일이므로 경로+파일이름으로 인제스터 생성
                    path += "/"+dumpFileName;
                    logger.info("file Path - Name  : {} - {}", path, dumpFileName);
                    ingester = new ProcedureIngester(path, dumpFormat, encoding, 1000, limitSize);
                }
            }else if (type.equals("procedure-link")) {

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

                String[] groupSeqLists = groupSeqs.split(",");

                // 1. groupSeqLists 수 만큼 프로시저 호출
                // 2. 체크
                // 3. 다 끝나면 rsync
                StringBuffer sb = new StringBuffer();

                for(String groupSeq : groupSeqLists){
                    logger.info("groupSeq : {}", groupSeq);
                    Integer groupSeqNumber = Integer.parseInt(groupSeq);
                    //프로시져
                    CallProcedure procedure = new CallProcedure(driverClassName, url, user, password, procedureName,groupSeqNumber,path, true);
                    //RSNYC
                    RsyncCopy rsyncCopy = new RsyncCopy(rsyncIp,rsyncPath,path,bwlimit,groupSeqNumber, true);

                    boolean execProdure = false;
                    boolean rsyncStarted = false;

                    //덤프파일 이름
                    String dumpFileName = "linkExt_"+groupSeq;
                    //SKIP 여부에 따라 프로시저 호출
                    if(procedureSkip == false) {
                        execProdure = procedure.callSearchProcedure();
                    }

                    logger.info("execProdure : {}",execProdure);

                    //프로시저 결과 True, R 스킵X or 프로시저 스킵 and rsync 스킵X
                    if((execProdure && rsyncSkip == false) || (procedureSkip && rsyncSkip == false)) {
                        rsyncCopy.start();
                        Thread.sleep(3000);
                        rsyncStarted = rsyncCopy.copyAsync();
                    }
                    logger.info("rsyncStarted : {}" , rsyncStarted );

                    if(rsyncStarted || rsyncSkip) {

                        if(rsyncSkip) {
                            logger.info("rsyncSkip : {}" , rsyncSkip);
                        }
                        //GroupSeq당 하나의 덤프파일이므로 경로+파일이름으로 인제스터 생성
                        String filepath = path + "/" + dumpFileName;
                        sb.append(filepath + ",");
                        logger.info("file Path - Name  : {} - {}", filepath, dumpFileName);
//                        ingester = new ProcedureIngester(filepath , dumpFormat, encoding, 1000, limitSize);
                    }
                }

                if(sb.charAt(sb.length()-1) == ','){
                    sb.deleteCharAt(sb.length()-1);
                }

                ingester = new ProcedureLinkIngester(sb.toString(), dumpFormat, encoding, 1000, limitSize);
            }

            Ingester finalIngester = ingester;
            Filter filter = (Filter) Utils.newInstance(filterClassName);
            service = new IndexService(host, port, scheme);

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
        } catch (Exception e) {
            job.setStatus(STATUS.ERROR.name());
            job.setError(e.getMessage());
            logger.error("error .... ", e);
        } finally {
            job.setEndTime(System.currentTimeMillis() / 1000);
        }
    }
}
