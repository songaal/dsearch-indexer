package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.ingester.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.origin.SystemEnvironmentOrigin;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.sql.rowset.CachedRowSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class CommandController {
    private static Logger logger = LoggerFactory.getLogger(CommandController.class);

    private static final String STATUS_READY = "READY";
    private static final String STATUS_RUNNING = "RUNNING";
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_ERROR = "ERROR";

    // 상태정보. 색인작업은 하나만 진행할 수 있다.
    private Map<String, Object> requestPayload = new HashMap<String, Object>();
    private String status = STATUS_READY;
    private String error;
    private IndexService service;

    private long startTime;
    private long endTime;

    @GetMapping(value = "/")
    public ResponseEntity<?> getDefault() {
        String responseBody = "FastcatX Indexer!";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }

    @PostMapping(value = "/shutdown")
    public ResponseEntity<?> shutdown() {
        System.exit(0);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    // 색인상태조회
    @GetMapping(value = "/status")
    public ResponseEntity<?> getStatus() {
        Map<String, Object> result = new HashMap<>();
        result.put("payload", requestPayload);
        //상태는 3가지 존재. running, finish, error
        result.put("status", status);
        if (error != null) {
            result.put("error", error);
        }
        if (startTime != 0) {
            result.put("startTime", startTime);
        }

        if (service != null) {
            // 정상종료와 에러발생시 endTime이 기록된다. 실행중이라면 비어있다.
            result.put("endTime", endTime);
            result.put("docSize", service.getCount());
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json; charset=UTF-8");
        return new ResponseEntity<>(result, headers, HttpStatus.OK);
    }

    /**
     * 색인 시작
     * {
     * "scheme": "http",
     * "host": "es1.danawa.io",
     * "port": 80,
     * "index": "song5",
     * "type": "ndjson",
     * "path": "C:\\Projects\\fastcatx-indexer\\src\\test\\resources\\sample.ndjson",
     * "encoding": "utf-8",
     * "bulkSize": 1000,
     * "reset": true
     * }
     */
    @PostMapping(value = "/start")
    public ResponseEntity<?> doStart(@RequestBody Map<String, Object> payload) throws InterruptedException {

        if (status.equals(STATUS_RUNNING)) {
            Map<String, Object> result = new HashMap<>();
            result.put("error", "STATUS is " + status);
            HttpHeaders headers = new HttpHeaders();
            headers.add("Content-Type", "application/json; charset=UTF-8");
            return new ResponseEntity<>(result, headers, HttpStatus.OK);
        }
        requestPayload = payload;

        // 공통
        // ES 호스트
        String host = (String) payload.get("host");
        // ES 포트
        Integer port = (Integer) payload.get("port");
        // http, https
        String scheme = (String) payload.get("scheme");

        // ES 유저
        String esUsername = (String) payload.get("esUsername");
        // ES 패스워드
        String esPassword = (String) payload.get("esPassword");

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

        /**
         * file기반 인제스터 설정
         */
        //파일 경로.
        String path = (String) payload.get("path");
        // 파일 인코딩. utf-8, cp949 등..
        String encoding = (String) payload.get("encoding");
        // 테스트용도로 데이터 갯수를 제한하고 싶을때 수치.
        Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

        // 초기화
        status = STATUS_RUNNING;
        service = null;
        long startNano = System.nanoTime();
        startTime = System.currentTimeMillis() / 1000;
        endTime = 0;
        error = "";

        Ingester ingester = null;
        try {
            if (type.equals("ndjson")) {
                ingester = new NDJsonIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("csv")) {
                ingester = new CSVIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("file")) {

                String headerText = (String) payload.get("headerText");
                String delimiter = (String) payload.get("delimiter");
                ingester = new DelimiterFileIngester(path, encoding, 1000, limitSize, headerText,delimiter);
            }else if (type.equals("jdbc")) {

                int sqlCount = 2;
                ArrayList<String> sqlList = new ArrayList<String>();

                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String dataSQL = (String) payload.get("dataSQL");

                sqlList.add(dataSQL);
                //dataSQL, dataSQL2, dataSQL3.. 있을 경우
                while ( payload.get("dataSQL" + String.valueOf(sqlCount)) != null ) {
                    sqlList.add((String) payload.get("dataSQL" + String.valueOf(sqlCount)));
                    sqlCount++;
                }

                Integer fetchSize = (Integer) payload.get("fetchSize");
                Integer maxRows = (Integer) payload.getOrDefault("maxRows", 0);
                Boolean useBlobFile = (Boolean) payload.getOrDefault("useBlobFile", false);
                ingester = new JDBCIngester(driverClassName, url, user, password, bulkSize, fetchSize, maxRows, useBlobFile, sqlList);
            } else if (type.equals("procedure")) {

                //프로시저 호출에 필요한 정보
                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String procedureName = (String) payload.getOrDefault("procedureName","PRSEARCHPRODUCT"); //PRSEARCHPRODUCT
                Integer groupSeq = (Integer) payload.get("groupSeq");
                String dumpFormat = (String) payload.get("dumpFormat"); //ndjson, konan
                String rsyncIp = (String) payload.get("rsyncIp"); // rsync IP
                String bwlimit = (String) payload.getOrDefault("bwlimit","0"); // rsync 전송속도 - 1024 = 1m/s
                boolean procedureSkip  = (Boolean) payload.getOrDefault("procedureSkip",false); // 프로시저 스킵 여부
                boolean rsyncSkip = (Boolean) payload.getOrDefault("rsyncSkip",false); // rsync 스킵 여부
                String rsyncPath = (String) payload.getOrDefault("rsyncPath", "search_data_alti");
                //String rsyncCommand = (String) payload.getOrDefault("rsyncCommand","rsync -av --inplace --progress --bwlimit=1000 --progress 192.168.4.198::search_data_alti/prodExt_0 /home/danawa/temp/");


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
                logger.info("rsyncStarted : {}", rsyncStarted );

                if(rsyncStarted || rsyncSkip) {
                    if(rsyncSkip) {
                        logger.info("rsyncSkip : {}" , rsyncSkip);
                    }
                    //GroupSeq당 하나의 덤프파일이므로 경로+파일이름으로 인제스터 생성
                    path += "/"+dumpFileName;
                    logger.info("file Path - Name  : {} - {}", path, dumpFileName);
                    ingester = new ProcedureIngester(path, dumpFormat, encoding, 1000, limitSize);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            status = STATUS_ERROR;
//            logger.error("Init error!", e);
            error = "Cannot establish jdbc connection.";
            endTime = System.currentTimeMillis() / 1000;
            return getStatus();
        }

        Ingester finalIngester = ingester;
        Filter filter = (Filter) Utils.newInstance(filterClassName);

        Thread t = new Thread(() -> {
            try {

                //프로시저 ingester의 경우...
                service = new IndexService(host, port, scheme,esUsername,esPassword);
                // 인덱스를 초기화하고 0건부터 색인이라면.
                if (reset) {
                    if (service.existsIndex(index)) {
                        service.deleteIndex(index);
                    }
                }
                if (threadSize > 1) {
                    service.indexParallel(finalIngester, index, bulkSize, filter, threadSize, pipeLine);
                } else {
                    service.index(finalIngester, index, bulkSize, filter, pipeLine);
                }

                status = STATUS_SUCCESS;

                service.getStorageSize(index);

            } catch (Exception e) {
                status = STATUS_ERROR;
//                logger.error("Indexing error!", e);
                error = e.toString();
            } finally {
                endTime = System.currentTimeMillis() / 1000;
            }
        });
        t.start();
        return getStatus();
    }
}

