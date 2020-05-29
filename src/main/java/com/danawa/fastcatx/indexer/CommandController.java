package com.danawa.fastcatx.indexer;

import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/")
public class CommandController {
    private static Logger logger = LoggerFactory.getLogger(CommandController.class);

    private static final String STATUS_RUNNING = "running";
    private static final String STATUS_FINISH = "finish";
    private static final String STATUS_ERROR = "error";
    @GetMapping(value = "/")
    public ResponseEntity<?> getDefault() {
        String responseBody = "FastcatX Indexer!";
        return new ResponseEntity<>(responseBody, HttpStatus.OK);
    }

    // 색인상태조회
    @GetMapping(value = "/status")
    public ResponseEntity<?> getStatus() {
        JsonObject obj =new JsonObject();
        //상태는 3가지 존재. running, finish, error
        obj.addProperty("status", STATUS_RUNNING);
        obj.addProperty("docSize", 1051);
        obj.addProperty("index", "test1");
        obj.addProperty("startTime", "2020-05-27T12:40:00+09:00");
        // 정상종료와 에러발생시 endTime이 기록된다. 실행중이라면 비어있다.
        obj.addProperty("endTime", "2020-05-27T14:40:00+09:00");
        return new ResponseEntity<>(obj.toString(), HttpStatus.OK);
    }

    /**
     *
     * 색인 시작
     *
     * {
     *     "scheme": "http",
     *     "host": "es1.danawa.io",
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

        // 공통
        String scheme = (String) payload.get("scheme");
        String host = (String) payload.get("host");
        Integer port = (Integer) payload.get("port");
        String index = (String) payload.get("index");
        String type = (String) payload.get("type");

        // NDJson
        String path = (String) payload.get("path");
        String encoding = (String) payload.get("encoding");
        Integer bulkSize = (Integer) payload.get("bulkSize");

        Ingester ingester = null;

        switch (type) {
            case "ndjson":
                ingester = new NDJsonIngester(path, encoding, 1000);
        }

        String error = "";
        try {
            RestHighLevelClient client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(host, port, scheme)));

            BulkRequest request = new BulkRequest();

            int count = 0;
            long totalTime = System.currentTimeMillis();
            long time = System.nanoTime();
            while (ingester.hasNext()) {
                count++;
                Map<String, Object> record = ingester.next();
//                logger.info("{}", record);

                request.add(new IndexRequest(index).source(record, XContentType.JSON));

                if (count % bulkSize == 0) {
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    logger.debug("bulk! {}", count);
                    request = new BulkRequest();
                }

                if (count % 100000 == 0) {
                    logger.info("{} ROWS FLUSHED! in {}ms", count, (System.nanoTime() - time) / 1000000);
                }
            }

            if (count % bulkSize > 0) {
                //나머지..
                BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                checkResponse(bulkResponse);
                logger.debug("Final bulk! {}", count);
            }

            logger.info("Flush Finished! doc[{}] elapsed[{}m]", count, (System.currentTimeMillis() - totalTime) / 1000 / 60);

        } catch (Exception e) {
            logger.error("Indexing start error!", e);
            error = e.toString();
        }

        // 결과  json
        JsonObject obj =new JsonObject();
        obj.addProperty("index", index);
        obj.addProperty("type", type);
        obj.addProperty("file", path);
        obj.addProperty("result", error.length() == 0 ? "success" : "fail");
        obj.addProperty("error", error);
        return new ResponseEntity<>(obj.toString(), HttpStatus.OK);
    }

    private void checkResponse(BulkResponse bulkResponse) {
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                if (bulkItemResponse.isFailed()) {
                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                    logger.error("Doc index error >> {}", failure);
                }
            }
        }
    }

}
