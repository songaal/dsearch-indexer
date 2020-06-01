package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.impl.NDJsonIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/")
public class CommandController {
    private static Logger logger = LoggerFactory.getLogger(CommandController.class);

    private static final String STATUS_READY = "ready";
    private static final String STATUS_RUNNING = "running";
    private static final String STATUS_FINISH = "finish";
    private static final String STATUS_ERROR = "error";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 상태정보. 색인작업은 하나만 진행할 수 있다.
    private Map<String, Object> requestPayload = new HashMap<String, Object>();
    private String status = STATUS_READY;
    private String error;
    private IndexService service;
    private String startTime;
    private String endTime = "";

    @GetMapping(value = "/")
    public ResponseEntity<?> getDefault() {
        String responseBody = "FastcatX Indexer!";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
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
        if (startTime != null) {
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
     *     "scheme": "http",
     *     "host": "es1.danawa.io",ZonedDateTime
     *     "port": 80,
     *     "index": "song5",
     *     "type": "ndjson",
     *     "path": "C:\\Projects\\fastcatx-indexer\\src\\test\\resources\\sample.ndjson",
     *     "encoding": "utf-8",
     *     "bulkSize": 1000
     * }
     */
    @PostMapping(value = "/start")
    public ResponseEntity<?> doStart(@RequestBody Map<String, Object> payload) {

        if (status.equals(STATUS_RUNNING)) {
            Map<String, Object> result = new HashMap<>();
            result.put("error", "Status is " + status);
            HttpHeaders headers = new HttpHeaders();
            headers.add("Content-Type", "application/json; charset=UTF-8");
            return new ResponseEntity<>(result, headers, HttpStatus.OK);
        }
        requestPayload = payload;

        // 공통
        String host = (String) payload.get("host");
        Integer port = (Integer) payload.get("port");
        String scheme = (String) payload.get("scheme");
        String index = (String) payload.get("index");
        String type = (String) payload.get("type");

        // file
        String path = (String) payload.get("path");
        String encoding = (String) payload.get("encoding");
        Integer bulkSize = (Integer) payload.get("bulkSize");

        Ingester ingester = null;

        switch (type) {
            case "ndjson":
                ingester = new NDJsonIngester(path, encoding, 1000);
        }

        // 초기화
        status = STATUS_RUNNING;
        service = null;
        startTime = LocalDateTime.now().format(formatter);
        endTime = "";
        error = "";

        Ingester finalIngester = ingester;

        Thread t = new Thread(() -> {
            try {
                service = new IndexService();
                service.index(finalIngester, host, port, scheme, index, bulkSize);
                status = STATUS_FINISH;
            } catch (Exception e) {
                status = STATUS_ERROR;
                logger.error("Indexing error!", e);
                error = e.toString();
            } finally {
                endTime = LocalDateTime.now().format(formatter);
            }
        });
        t.start();

        // 결과  json
        return getStatus();
    }

}
