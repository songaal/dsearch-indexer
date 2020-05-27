package com.danawa.fastcatx.indexer;

import com.google.gson.JsonObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

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
    public ResponseEntity<?> getStatus(HttpServletRequest request,
                                   @RequestParam Map<String,String> queryStringMap,
                                   @RequestBody(required = false) byte[] body) {
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

    // 색인 시작
    @PostMapping(value = "/start")
    public ResponseEntity<?> doStart(HttpServletRequest request,
                                       @RequestParam Map<String,String> queryStringMap,
                                       @RequestBody(required = false) byte[] body) {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http"),
                        new HttpHost("localhost", 9201, "http")));

        // 리스너
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {

            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {

            }

            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests",
                        executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    logger.warn("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds",
                            executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };

        // 벌크 프로세스 빌더
        BulkProcessor bulkProcessor = BulkProcessor.builder(
                client,
               listener)
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(1)
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        //색인
        IndexRequest one = new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "title",
                        "In which order are my Elasticsearch queries executed?");
        IndexRequest two = new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "title",
                        "Current status and upcoming changes in Elasticsearch");
        IndexRequest three = new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "title",
                        "The Future of Federated Search in Elasticsearch");

        bulkProcessor.add(one);
        bulkProcessor.add(two);
        bulkProcessor.add(three);

        // 기다림.
        boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);

        // 결과  json
        JsonObject obj =new JsonObject();
        obj.addProperty("index", "test1");
        obj.addProperty("type", "file");
        obj.addProperty("file", "/home/search/doc.txt");
        obj.addProperty("result", "success");
        obj.addProperty("error", "");
        return new ResponseEntity<>(obj.toString(), HttpStatus.OK);
    }

}
