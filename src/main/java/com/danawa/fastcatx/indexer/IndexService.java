package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class IndexService {

    private static Logger logger = LoggerFactory.getLogger(IndexService.class);

    final int SOCKET_TIMEOUT = 10 * 60 * 1000;
    final int CONNECTION_TIMEOUT = 40 * 1000;
    final int MAX_RETRIES_COUNT = 5;
    final int RETRY_INTERVAL_MS = 1000;

    // 몇건 색인중인지.
    private int count;

    //ES 연결정보.
    private String host;
    private Integer port;
    private String scheme;
    private RestClientBuilder restClientBuilder;
    private Integer groupSeq;

    private ConnectionKeepAliveStrategy getConnectionKeepAliveStrategy() {
        return (response, context) -> 10 * 60 * 1000;
    }

    public IndexService(String host, Integer port, String scheme) {
       this(host, port, scheme, null, null);
    }

    public IndexService(String host, Integer port, String scheme, String esUsername, String esPassword) {
        this.host = host;
        this.port = port;
        this.scheme = scheme;
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (esUsername != null && !"".equals(esUsername) && esPassword != null && !"".equals(esPassword)) {
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esUsername, esPassword));
            this.restClientBuilder = RestClient.builder(new HttpHost[]{new HttpHost(host, port, scheme)}).setHttpClientConfigCallback((httpClientBuilder) -> {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider).setKeepAliveStrategy(this.getConnectionKeepAliveStrategy());
            });
        } else {
            this.restClientBuilder = RestClient.builder(new HttpHost[]{new HttpHost(host, port, scheme)})
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                            .setSocketTimeout(SOCKET_TIMEOUT)).setHttpClientConfigCallback((httpClientBuilder) -> {
                return httpClientBuilder.setKeepAliveStrategy(this.getConnectionKeepAliveStrategy());
            });
        }

        this.restClientBuilder.setRequestConfigCallback((requestConfigBuilder) -> {
            return requestConfigBuilder.setConnectTimeout(40000).setSocketTimeout(600000);
        });
        logger.info("host : {} , port : {}, scheme : {}, username: {}, password: {} ", new Object[]{host, port, scheme, esUsername, esPassword});
    }
    public IndexService(String host, Integer port, String scheme, String esUsername, String esPassword, int groupSeq) {
        this(host, port, scheme, esUsername, esPassword);
        this.groupSeq = groupSeq;
    }

    public int getCount() {
        return count;
    }

    public boolean existsIndex(String index) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            GetIndexRequest request = new GetIndexRequest(index);
            return client.indices().exists(request, RequestOptions.DEFAULT);

        } catch (IOException e) {
            logger.error("", e);
            throw e;
        }
    }

    public boolean deleteIndex(String index) throws IOException {
        boolean flag = false;
        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            flag = deleteIndexResponse.isAcknowledged();
        }catch (Exception e){
            logger.error("", e);
        }

        return flag;
    }

    public boolean createIndex(String index, Map<String, ?> settings) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(settings);
            AcknowledgedResponse deleteIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            return deleteIndexResponse.isAcknowledged();
        }
    }

    public void index(Ingester ingester, String index, Integer bulkSize, Filter filter, String pipeLine) throws CircuitBreakerException, Exception, StopSignalException {
        index(ingester, index, bulkSize, filter, null, pipeLine);
    }

    public void index(Ingester ingester, String index, Integer bulkSize, Filter filter, Job job, String pipeLine) throws CircuitBreakerException, Exception, StopSignalException {
        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            count = 0;

            String id = "";
            long start = System.currentTimeMillis();
            BulkRequest request = new BulkRequest();
            long time = System.nanoTime();
            try {
                logger.info("index start!");

                while (ingester.hasNext()) {
                    if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                        logger.info("Stop Signal");
                        throw new StopSignalException();
                    }

                    Map<String, Object> record = ingester.next();
                    if (filter != null && record != null && record.size() > 0) {
                        record = filter.filter(record);
                    }

                    //logger.info("{}", record);
                    if (record != null && record.size() > 0) {
                        count++;

                        IndexRequest indexRequest = new IndexRequest(index).source(record, XContentType.JSON);

                        if (record.get("ID") != null) {
                            id = record.get("ID").toString();
                        } else if (record.get("id") != null) {
                            id = record.get("id").toString();
                        }

                        if (id.length() > 0) {
                            indexRequest.id(id);
                        }

                        if (pipeLine.length() > 0) {
                            indexRequest.setPipeline(pipeLine);
                        }

                        request.add(indexRequest);
                    }

                    if (count % bulkSize == 0) {
                        int waitCount = 10;
                        int sleepTime = 30000;

                        // 문서 손실 방지. es rejected 에러시 무한 색인 요청 -> 무한 색인에서 총 10회, 30초 대기로 변경(circuit breaker exception 오류)
                        while (true) {
                            if(waitCount == 0){
                                throw new CircuitBreakerException("서킷 브레이커 익셉션이 발생 하였습니다.");
                            }

                            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                            boolean isCircuitBreakerException = false; // 서킷 브레이커 익셉션 발생 판단

                            if (bulkResponse.hasFailures()) {
                                BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                                for (int i = bulkItemResponses.length - 1; i >= 0; i--) {
                                    BulkItemResponse bulkItemResponse = bulkItemResponses[i];

                                    if (bulkItemResponse.isFailed()) {
                                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                        // write queue reject 이슈 코드 = ( 429 )
                                        // 서킷 브레이커 익셉션 발생으로 판단
                                        if (failure.getStatus() == RestStatus.fromCode(429)) {
                                            logger.error("write queue rejected!! >> {}", failure);
                                            isCircuitBreakerException = true;
                                        } else {
                                            // 색인 시 형태소 분석 에러나 디스크 용량이 꽉 찼을 경우 에러 발생
                                            logger.error("Doc index error >> {}", failure);

                                            // bulk request 에서 지워준다
                                            request.requests().remove(i);
                                        }
                                    }else{
                                        request.requests().remove(i);
                                    }
                                }
                            } else {
                                // bulk request에 대한 실패가 없음
                                request.requests().clear();
                            }

                            if(isCircuitBreakerException){
                                // 서킷 브레이커 익셉션이 발생했을 경우
                                // 30초 대기, 10회
                                Thread.sleep(sleepTime);
                                waitCount--;
                                logger.info("Circuit Breaker Exception Count... {}", waitCount);
                                continue;
                            }

                            if(request.requests().size() == 0) break;

                            if (job.getStopSignal()) {
                                throw new StopSignalException();
                            }
                        }
                    }

                    if (count != 0 && count % 10000 == 0) {
                        if (groupSeq != null) {
                            logger.info("GroupSeq: [{}], index: [{}] {} ROWS FLUSHED! in {}ms", groupSeq, index, count, (System.nanoTime() - time) / 1000000);
                        } else {
                            logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
                        }
                    }
                    //logger.info("{}",count);
                }

                if (request.requests().size() > 0) {
                    //나머지..
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    checkResponse(bulkResponse);
                    logger.info("Final bulk! {}", count);
                }

            } catch (StopSignalException e) {
                throw e;
            } catch (CircuitBreakerException e){
                throw e;
            } catch (Exception e) {
                logger.error("Error occur! => ", e);

                // 에러가 났을 경우 request 안의 id와 type을 10 개만 찍는다
                List<DocWriteRequest<?>> list = request.requests();
                int errorCount = list.size() < 10 ? list.size() : 10;
                for(int i = 0 ;i < errorCount; i++){
                    DocWriteRequest docWriteRequest = list.get(i);
                    logger.error("request => ", docWriteRequest.id(), docWriteRequest.type());
                }
                throw e;
            }

            long totalTime = System.currentTimeMillis() - start;
            if (groupSeq != null) {
                logger.info("GroupSeq: [{}], index:[{}] Flush Finished! doc[{}] elapsed[{}m]", groupSeq, index, count, totalTime / 1000 / 60);
            } else {
                logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
            }
        }
    }

    /*
    * reindex 전체색인을 위한 메소드
     */
    public void reindex(Map<String, Object> payload, String destIndex, Job job) throws Exception {
        // dest로 source를 구함
        String sourceIndex = destIndex.endsWith("-a") ? destIndex.replace("-a", "-b") : destIndex.replace("-b", "-a"); // 목적지 인덱스
        String slices = (String) payload.get("slices");
        String reindexCheckMs = (String) payload.get("reindexCheckIntervalMs");
        String replicaCheckMs = (String) payload.get("replicaCheckIntervalMs");

        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            long start = System.currentTimeMillis();
            try {
                // 레플리카를 0으로 바꾸기 전 셋팅 가져오기
                int replicaCount = getIndexReplicaSetting(client, destIndex);

                // 대상 인덱스 replica 셋팅 0으로 변경
                modifyReplica(client, destIndex, 0);

                // reindex 작업 시작
                String taskId = executeReindex(client, sourceIndex, destIndex, slices);

                // reindex가 끝났는지 반복 확인
                while (!isTaskDone(client, taskId)) {
                    if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                        logger.info("Stop Signal");
                        cancelReindexTask(client, taskId);
                        throw new StopSignalException();
                    }

                    Thread.sleep(Long.parseLong(reindexCheckMs));
                }

                // 레플리카 생성 시작
                modifyReplica(client, destIndex, replicaCount);

                // 레플리카 생성 후 잠깐 대기 10초
                Thread.sleep(10000);

                // 레플리카 생성이 끝났는지 반복 확인
                while (!isIndexGreen(client, destIndex)) {
                    if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                        logger.info("Stop Signal");
                        throw new StopSignalException();
                    }

                    Thread.sleep(Long.parseLong(replicaCheckMs));
                }

            } catch (StopSignalException e) {
                logger.error("Process Stop! => ", e);
                throw e;
            } catch (Exception e) {
                logger.error("Error occur! => ", e);
                throw e;
            }

            long totalTime = System.currentTimeMillis() - start;
            logger.info("index:[{}] reindex Finished! doc[{}] elapsed[{}m]", destIndex, count, totalTime / 1000 / 60);
        }
    }

    private int getIndexReplicaSetting(RestHighLevelClient client, String index) {
        int count = 0;
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                RestClient restClient = client.getLowLevelClient();
                Request request = new Request(
                        "GET",
                        index + "/_settings");
                Response response = restClient.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());

                // 다중으로 존재하므로 아래와 같이 파싱..
                JSONObject jsonObj = new JSONObject(responseBody);
                JSONObject targetIndex = jsonObj.getJSONObject(index);
                JSONObject settings = targetIndex.getJSONObject("settings");
                JSONObject config = settings.getJSONObject("index");
                count = config.getInt("number_of_replicas");
                logger.info("TARGET_INDEX_REPLICA_COUNT : {}", count);
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("GET_REPLICA_SETTING_FAIL. {} RETRY : {}", e, i);
                try {
                    // 재시도하기 전에 3분 동안 휴면
                    Thread.sleep(RETRY_INTERVAL_MS * 180);
                } catch (Exception ignore){}
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new RuntimeException("GET_SETTING_FAIL_ERROR", e);
                }
            }
        }
        return count;
    }

    // reindex API
    private String executeReindex(RestHighLevelClient client, String sourceIndex, String destIndex, String slices) throws Exception {
        String taskId = null;
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                RestClient restClient = client.getLowLevelClient();

                // reindex API 호출
                Request request = new Request(
                        "POST",
                        "/_reindex");
                request.addParameter("wait_for_completion", "false");
                request.addParameter("slices", slices);
                String entity = "{\n" +
                        "  \"source\": {\n" +
                        "    \"index\": \"" + sourceIndex + "\"\n" +
                        "  }, \n" +
                        "  \"dest\": {\n" +
                        "    \"index\": \"" + destIndex + "\"\n" +
                        "  } \n" +
                        "}";
                request.setJsonEntity(entity);
                Response response = restClient.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObj = new JSONObject(responseBody);
                logger.info("REINDEX_START : {}, SOURCE_INDEX : {}, DEST_INDEX : {}, SLICES : {}", jsonObj, sourceIndex, destIndex, slices);
                if(jsonObj.get("task") != null){
                    taskId = (String) jsonObj.get("task");
                }
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("EXECUTE_REINDEX_FAIL. {} RETRY : {}", e, i);
                // 재시도하기 전에 1초 동안 휴면
                Thread.sleep(RETRY_INTERVAL_MS);
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new RuntimeException("REINDEX_EXECUTE_ERROR : ", e);
                }
            }
        }
        return taskId;
    }

    // reindex cancel API
    private void cancelReindexTask(RestHighLevelClient client, String taskId) throws Exception {
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                RestClient restClient = client.getLowLevelClient();
                Request request = new Request(
                        "POST",
                        "_tasks/" + taskId +"/_cancel");
                Response response = restClient.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObj = new JSONObject(responseBody);
                logger.info("REINDEX_CANCEL : {}, TASK_ID : {}", jsonObj, taskId);
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("CANCEL_REINDEX_FAIL. {} RETRY : {}", e, i);
                // 재시도하기 전에 1초 동안 휴면
                Thread.sleep(RETRY_INTERVAL_MS);
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new Exception("REINDEX_CANCEL_ERROR : ", e);
                }
            }
        }
    }

    private void modifyReplica(RestHighLevelClient client, String destIndex, int size) throws Exception {
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                RestClient restClient = client.getLowLevelClient();
                Request request = new Request(
                        "PUT",
                        "/" + destIndex + "/_settings");
                String entity = "{\n" +
                        "  \"index\": {\n" +
                        "    \"number_of_replicas\": \"" + size + "\"\n" +
                        "  } \n" +
                        "}";
                request.setJsonEntity(entity);
                Response response = restClient.performRequest(request);
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject jsonObj = new JSONObject(responseBody);
                logger.info("Modify Replica : {}, TARGET_INDEX : {}, SIZE : {}", jsonObj, destIndex, size);
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("MODIFY_REPLICA_FAIL. {} RETRY : {}", e, i);
                // 재시도하기 전에 1초 동안 휴면
                Thread.sleep(RETRY_INTERVAL_MS);
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new Exception("MODIFY_REPLICA_ERROR : ", e);
                }
            }
        }
    }

    private boolean isIndexGreen(RestHighLevelClient client, String index) throws Exception {
        boolean result = false;
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                if(index != null){
                    RestClient restClient = client.getLowLevelClient();
                    Request request = new Request(
                            "GET",
                            "/"+ index + "/_shard_stores/");
                    Response response = restClient.performRequest(request);
                    String responseBody = EntityUtils.toString(response.getEntity());
                    JSONObject jsonObj = new JSONObject(responseBody);

                    // 결과값이 없을때 Green 상태 processing false.
                    result = jsonObj.get("indices").toString().equals("{}");
                }
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("CHECK_INDEX_GREEN_FAIL. {} RETRY : {}", e, i);
                // 재시도하기 전에 1초 동안 휴면
                Thread.sleep(RETRY_INTERVAL_MS);
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new Exception("CHECK_INDEX_GREEN_ERROR : ", e);
                }
            }
        }
        return result;
    }

    private boolean isTaskDone(RestHighLevelClient client, String taskId) throws Exception {
        boolean result = false;
        for (int i = 0; i <= MAX_RETRIES_COUNT; i++) {
            try{
                if(taskId != null){
                    RestClient restClient = client.getLowLevelClient();
                    Request request = new Request(
                            "GET",
                            "/_tasks/" + taskId);
                    Response response = restClient.performRequest(request);
                    String responseBody = EntityUtils.toString(response.getEntity());
                    JSONObject jsonObj = new JSONObject(responseBody);
                    result = ((boolean) jsonObj.get("completed"));
                }
                break;
            } catch (Exception e) {
                // 예외 처리
                logger.error("TASK_CHECK_FAIL. {} RETRY : {}", e, i);
                // 재시도하기 전에 1초 동안 휴면
                Thread.sleep(RETRY_INTERVAL_MS);
                // 마지막 재시도가 실패하면 예외를 던진다.
                if (i == MAX_RETRIES_COUNT) {
                    throw new Exception("TASK_CHECK_FAIL ERROR : ",  e);
                }
            }
        }
        return result;
    }

    class Worker implements Callable {
        private BlockingQueue queue;
        private int sleepTime = 30000;
        private String index;
        private Job job;

        public Worker(BlockingQueue queue, String index, Job job) {
            this.queue = queue;
            this.index = index;
            this.job = job;
        }

        private void retry(BulkRequest bulkRequest) {
            // 동기 방식
            BulkResponse bulkResponse = null;

            try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
                // 문서 손실 방지. es rejected 에러시 무한 색인 요청 -> 무한 색인에서 총 10회, 30초 대기로 변경(circuit breaker exception 오류)
                int waitCount = 10;

                while (true) {
                    if(waitCount == 0){
                        throw new CircuitBreakerException("서킷 브레이커 익셉션이 발생했습니다.");
                    }

                    bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    boolean isCircuitBreakerException = false; // 서킷 브레이커 익셉션 발생 판단

                    if (bulkResponse.hasFailures()) {
                        BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                        // 각 인덱스 리퀘스트 별 리스폰스 분석
                        for (int i = bulkItemResponses.length - 1; i >= 0 ; i--) {
                            BulkItemResponse bulkItemResponse = bulkItemResponses[i];
                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

                                // write queue reject 이슈 코드 = ( 429 )
                                // 서킷 브레이커 익셉션 발생으로 판단
                                if (failure.getStatus() == RestStatus.fromCode(429)) {
                                    logger.error("write queue rejected!! >> {}", failure);
                                    isCircuitBreakerException = true;
                                } else {
                                    // 색인 시 형태소 분석 에러나 디스크 용량이 꽉 찼을 경우 에러 발생
                                    logger.error("Doc index error >> {}", failure);

                                    // bulk request 에서 지워준다
                                    bulkRequest.requests().remove(i);
                                }
                            }else{
                                // 실제로 색인이 된 리퀘스트는 제외 한다.
                                bulkRequest.requests().remove(i);
                            }
                        }
                    }else{
                        // 정상적으로 성공됨
                        bulkRequest.requests().clear();
                    }

                    if(isCircuitBreakerException){
                        // 서킷 브레이커 익셉션이 발생했을 경우
                        // 30초 대기, 10회
                        Thread.sleep(sleepTime);
                        waitCount--;
                        logger.info("Circuit Breaker Exception Count... {}", waitCount);
                        continue;
                    }

                    if(bulkRequest.requests().size() == 0) break;

                    if (job.getStopSignal()) {
                        throw new StopSignalException();
                    }
                }
            } catch (Exception e) {
                // exception 발생 시 무한 색인 요청(while)을 빠져나간다.
                logger.error(e.getMessage());
            }
            logger.debug("Bulk Success : index:{} - count:{}, - elapsed:{}", index, count, bulkResponse.getTook());
        }


        @Override
        public Object call() {
            try {
                while (true) {
                    Object o = queue.take();
                    if (o instanceof String) {
                        //종료.
                        logger.info("Indexing Worker-{} index={} got {}", Thread.currentThread().getId(),index,o);
                        break;
                    }
                    BulkRequest request = (BulkRequest) o;
                    retry(request);
                    logger.debug("remained queue : {}", queue.size());
                }
            } catch (Throwable e) {
                logger.error("indexParallel : {}", e);
            }
            return null;
        }
    }

    public void indexParallel(Ingester ingester, String index, Integer bulkSize, Filter filter, int threadSize, String pipeLine) throws IOException, StopSignalException, InterruptedException {
        indexParallel(ingester, index, bulkSize, filter, threadSize, null, pipeLine);
    }

    public void indexParallel(Ingester ingester, String index, Integer bulkSize, Filter filter, int threadSize, Job job, String pipeLine) throws IOException, StopSignalException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);

        BlockingQueue queue = new LinkedBlockingQueue(threadSize * 10);
        //여러 쓰레드가 작업큐를 공유한다.
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            Worker w = new Worker(queue, index, job);
            list.add(executorService.submit(w));
        }

        String id = "";
        count = 0;
        long start = System.currentTimeMillis();
        BulkRequest request = new BulkRequest();
        long time = System.nanoTime();
        try {
            while (ingester.hasNext()) {
                if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                    logger.info("Stop Signal");
                    throw new StopSignalException();
                }

                Map<String, Object> record = ingester.next();

                //logger.info("record : {}" ,record.size());

                if (filter != null && record != null && record.size() > 0) {
                    record = filter.filter(record);
                }

                if (record != null && record.size() > 0) {
                    count++;

                    IndexRequest indexRequest = new IndexRequest(index).source(record, XContentType.JSON);

                    //_id 자동생성이 아닌 고정 필드로 색인
                    if (record.get("ID") != null) {
                        id = record.get("ID").toString();
                    } else if (record.get("id") != null) {
                        id = record.get("id").toString();
                    }

                    if (id.length() > 0) {
                        indexRequest.id(id);
                    }

                    if (pipeLine.length() > 0) {
                        indexRequest.setPipeline(pipeLine);
                    }

                    request.add(indexRequest);
                }

                if (count % bulkSize == 0) {
                    queue.put(request);

//                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                    logger.debug("bulk! {}", count);

                    request = new BulkRequest();
                }

                if (count % 100000 == 0) {
                    if (groupSeq != null) {
                        logger.info("groupSeq: [{}], index: [{}] {} ROWS FLUSHED! in {}ms", groupSeq, index, count, (System.nanoTime() - time) / 1000000);
                    } else {
                        logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
                    }
                }
            }

            if (request.estimatedSizeInBytes() > 0) {
                //나머지..
                queue.put(request);
//                BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                checkResponse(bulkResponse);
                logger.debug("Final bulk! {}", count);
            }


        } catch (InterruptedException e) {
            logger.error("interrupted! ", e);
            throw e;
        } catch (Exception e) {
            logger.error("[Exception] ", e);
            throw e;
        } finally {
            try {
                for (int i = 0; i < list.size(); i++) {
                    // 쓰레드 갯수만큼 종료시그널 전달.
                    queue.put("<END>");
                }

            } catch (InterruptedException e) {
                logger.error("", e);
                //ignore
            }

            try {
                for (int i = 0; i < list.size(); i++) {
                    Future f = list.get(i);
                    f.get();
                }
            } catch (Exception e) {
                logger.error("", e);
                //ignore
            }

            // 큐 안의 내용 제거
            logger.info("queue clear");
            queue.clear();

            // 쓰레드 종료
            logger.info("{} thread shutdown", index);
            executorService.shutdown();

            // 만약, 쓰레드가 정상적으로 종료 되지 않는다면,
            if (!executorService.isShutdown()) {
                // 쓰레드 강제 종료
                logger.info("{} thread shutdown now!", index);
                executorService.shutdownNow();
            }
        }

        long totalTime = System.currentTimeMillis() - start;
        if (groupSeq != null) {
            logger.info("GroupSeq: [{}],index:[{}] Flush Finished! doc[{}] elapsed[{}m]",groupSeq, index, count, totalTime / 1000 / 60);
        } else {
            logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
        }
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

    public String getStorageSize() throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(restClientBuilder)) {
            StatsRequest statsRequest = new StatsRequest();
            StatsResponse statsResponse = client.enrich().stats(statsRequest, RequestOptions.DEFAULT);
            return null;
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return null;
    }

    public boolean retryUntilCreatingIndex(String index, Map<String, Object> indexSettings, int retryCount) throws IOException, InterruptedException {
        boolean result = false;

        for (int r = 0; r < retryCount; r ++) {
            boolean isExistsIndex = existsIndex(index);
            logger.info("[{}] existsIndex: {}", index, isExistsIndex);
            if (isExistsIndex) {
                // 기존 인덱스가 존재할때 지우고 다시 생성
                boolean isDeleteIndex = deleteIndex(index);
                logger.info("[{}] isDeleteIndex: {}", index, isDeleteIndex);
                if(isDeleteIndex) {
                    if (createIndex(index, indexSettings)) {
                        // 생성 플래그값
                        result = true;
                        break;
                    }
                }
            } else {
                // 기존 인덱스가 없으면 생성
                if (createIndex(index, indexSettings)) {
                    // 생성 플래그값
                    result = true;
                    break;
                }
            }
            Thread.sleep(500);
        }
        return result;
    }

    public RestClientBuilder getRestClientBuilder() {
        return this.restClientBuilder;
    }
}
