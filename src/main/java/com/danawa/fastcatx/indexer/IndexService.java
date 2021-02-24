package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexService {

    private static Logger logger = LoggerFactory.getLogger(IndexService.class);

    final int SOCKET_TIMEOUT = 10 * 60 * 1000;
    final int CONNECTION_TIMEOUT = 40 * 1000;

    // 몇건 색인중인지.
    private int count;

    //ES 연결정보.
    private String host;
    private Integer port;
    private String scheme;

    private ConnectionKeepAliveStrategy getConnectionKeepAliveStrategy() {
        return (response, context) -> 10 * 60 * 1000;
    }

    public IndexService(String host, Integer port, String scheme) {
        this.host = host;
        this.port = port;
        this.scheme = scheme;
        logger.info("host : {} , port : {}, scheme : {} ", host, port, scheme);
    }

    public int getCount() {
        return count;
    }

    public boolean existsIndex(String index) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)))) {
            GetIndexRequest request = new GetIndexRequest(index);
            return client.indices().exists(request, RequestOptions.DEFAULT);

        } catch (IOException e) {
            logger.error("", e);
            throw e;
        }
    }

    public boolean deleteIndex(String index) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                        .setSocketTimeout(SOCKET_TIMEOUT))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setKeepAliveStrategy(getConnectionKeepAliveStrategy())))) {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            return deleteIndexResponse.isAcknowledged();
        }
    }

    public boolean createIndex(String index, Map<String, ?> settings) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                        .setSocketTimeout(SOCKET_TIMEOUT))
                .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setKeepAliveStrategy(getConnectionKeepAliveStrategy())))) {
            CreateIndexRequest request = new CreateIndexRequest(index);
            request.settings(settings);
            AcknowledgedResponse deleteIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            return deleteIndexResponse.isAcknowledged();
        }
    }


    public void index(Ingester ingester, String index, Integer bulkSize, Filter filter, String pipeLine) throws IOException, StopSignalException {
        index(ingester, index, bulkSize, filter, null, pipeLine);
    }

    public void index(Ingester ingester, String index, Integer bulkSize, Filter filter, Job job, String pipeLine) throws IOException, StopSignalException {
        try (
                RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme))
                        .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                                .setSocketTimeout(SOCKET_TIMEOUT))
                        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                                .setKeepAliveStrategy(getConnectionKeepAliveStrategy())))) {


            count = 0;

            String id = "";
            long start = System.currentTimeMillis();
            BulkRequest request = new BulkRequest();
            long time = System.nanoTime();
            try {

                logger.info("index 시작");

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
                        // 기존 소스
//                        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                        checkResponse(bulkResponse);

                        // 동기 방식
                        boolean doRetry = false;
                        BulkRequest retryBulkRequest = new BulkRequest();
                        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                        if (bulkResponse.hasFailures()) {
                            // bulkResponse에 에러가 있다면
                            // retry 1회 시도
                            doRetry = true;
                            BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                            List<DocWriteRequest<?>> requestList = request.requests();
                            for (int i = 0; i < bulkItemResponses.length; i++) {
                                BulkItemResponse bulkItemResponse = bulkItemResponses[i];

                                if (bulkItemResponse.isFailed()) {
                                    BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

                                    // write queue reject 이슈 코드 = ( 429 )
                                    if (failure.getStatus() == RestStatus.fromCode(429)) {
                                        logger.error("write queue rejected!! >> {}", failure);

                                        // retry bulk request에 추가
                                        // bulkRequest에 대한 response의 순서가 동일한 샤드에 있다면 보장.
                                        // https://discuss.elastic.co/t/is-the-execution-order-guaranteed-in-a-single-bulk-request/100412

                                        retryBulkRequest.add(requestList.get(i));
//                                        logger.debug("retryBulkRequest add : {}", requestList.get(i));
                                    } else {
                                        logger.error("Doc index error >> {}", failure);
                                    }
                                }
                            }
                        }

                        // 재시도 로직 - 1회만 재시도.
                        // 그러나, retryBulkRequest에 추가된 request가 없다면 에러 발생.
                        if (doRetry && retryBulkRequest.requests().size() > 0) {
//                            logger.debug("retry bulk requests size : {}", retryBulkRequest.requests().size());
                            bulkResponse = client.bulk(retryBulkRequest, RequestOptions.DEFAULT);
                            if (bulkResponse.hasFailures() ) {
                                BulkItemResponse[] responses = bulkResponse.getItems();
                                for (int i = 0; i < responses.length; i++) {
                                    BulkItemResponse bulkItemResponse = responses[i];

                                    if (bulkItemResponse.isFailed()) {
                                        BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

                                        if (failure.getStatus() == RestStatus.fromCode(429)) {
                                            logger.error("retryed, but write queue rejected!! >> {}", failure);
                                        } else {
                                            logger.error("retryed, but Doc index error >> {}", failure);
                                        }
                                    }
                                }
                            }
                        }

//                        logger.debug("bulk! {}", count);
                        request = new BulkRequest();
                    }

                    if (count % 10000 == 0) {
                        logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
                    }
                    //logger.info("{}",count);
                }

                if (request.estimatedSizeInBytes() > 0) {
                    //나머지..
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    checkResponse(bulkResponse);
                    logger.debug("Final bulk! {}", count);
                }

            } catch (StopSignalException e) {
                throw e;
            } catch (Exception e) {
                logger.info("{}", e);
                StackTraceElement[] exception = e.getStackTrace();
                logger.error("[Exception] : request id : {}", id);
                for (StackTraceElement element : exception) {
                    e.printStackTrace();
                    logger.error("[Exception] : " + element.toString());
                }
            }

            long totalTime = System.currentTimeMillis() - start;
            logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
        }
    }

    class Worker implements Callable {
        private BlockingQueue queue;
        private RestHighLevelClient client;
        private int sleepTime = 1000;
        private boolean isStop = false;
        private String index;

        public Worker(BlockingQueue queue, String index) {
            this.queue = queue;
            this.index = index;
            this.client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                            .setSocketTimeout(SOCKET_TIMEOUT))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setKeepAliveStrategy(getConnectionKeepAliveStrategy())));
        }

        private void retry(BulkRequest bulkRequest) {
            // 동기 방식
            boolean doRetry = false;
            try {
                BulkRequest retryBulkRequest = new BulkRequest();

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkResponse.hasFailures()) {
                    // bulkResponse에 에러가 있다면
                    // retry 1회 시도
                    logger.error("BulkRequest Error : {}", bulkResponse.buildFailureMessage());
                    doRetry = true;
                    BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                    List<DocWriteRequest<?>> requests = bulkRequest.requests();
                    logger.info("requests : {}", requests.size());
                    for (int i = 0; i < bulkItemResponses.length; i++) {
                        BulkItemResponse bulkItemResponse = bulkItemResponses[i];

                        if (bulkItemResponse.isFailed()) {
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

                            // write queue reject 이슈 코드 = ( 429 )
                            if (failure.getStatus() == RestStatus.fromCode(429)) {
                                logger.error("write queue rejected!! >> {}", failure);

                                // retry bulk request에 추가
                                // bulkRequest에 대한 response의 순서가 동일한 샤드에 있다면 보장.
                                // https://discuss.elastic.co/t/is-the-execution-order-guaranteed-in-a-single-bulk-request/100412

                                retryBulkRequest.add(requests.get(i));
                            } else {
                                logger.error("Doc index error >> {}", failure);
                            }
                        }
                    }
                }

                // 재시도 로직 - 1회만 재시도.
                if (doRetry && retryBulkRequest.requests().size() > 0) {
                    bulkResponse = client.bulk(retryBulkRequest, RequestOptions.DEFAULT);
                    if (bulkResponse.hasFailures()) {
                        BulkItemResponse[] responses = bulkResponse.getItems();
                        for (int i = 0; i < responses.length; i++) {
                            BulkItemResponse bulkItemResponse = responses[i];

                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();

                                if (failure.getStatus() == RestStatus.fromCode(429)) {
                                    logger.error("retryed, but write queue rejected!! >> {}", failure);
                                } else {
                                    logger.error("retryed, but Doc index error >> {}", failure);
                                }
                            }
                        }
                    }
                }

                logger.debug("Bulk Success : index:{} - count:{}, - elapsed:{}", index, count, bulkResponse.getTook());

            } catch (Exception e) {
                logger.error("retry : {}", e);
            }
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
//                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                    checkResponse(bulkResponse);
                    retry(request);
                    logger.debug("remained queue : {}", queue.size());
//                logger.debug("bulk! {}", count);
                }
            } catch (Throwable e) {
                logger.error("indexParallel : {}", e);
            }

            // 기존 소스
//            while(true) {
//                Object o = queue.take();
//                if (o instanceof String) {
//                    //종료.
//                    logger.info("Indexing Worker-{} got {}", Thread.currentThread().getId(), o);
//                    break;
//                }
//
//                BulkRequest request = (BulkRequest) o;
//                BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                checkResponse(bulkResponse);
////                logger.debug("bulk! {}", count);
//            }
            return null;
        }
    }

    public void indexParallel(Ingester ingester, String index, Integer bulkSize, Filter filter, int threadSize, String pipeLine) throws IOException, StopSignalException {
        indexParallel(ingester, index, bulkSize, filter, threadSize, null, pipeLine);
    }

    public void indexParallel(Ingester ingester, String index, Integer bulkSize, Filter filter, int threadSize, Job job, String pipeLine) throws IOException, StopSignalException {
        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);

        BlockingQueue queue = new LinkedBlockingQueue(threadSize * 10);
        //여러 쓰레드가 작업큐를 공유한다.
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            Worker w = new Worker(queue, index);
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
                    logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
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
        } catch (Exception e) {
            logger.error("[Exception] ", e);
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
        logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
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

    public String getStorageSize(String index) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)));
        StatsRequest statsRequest = new StatsRequest();
        StatsResponse statsResponse =
                client.enrich().stats(statsRequest, RequestOptions.DEFAULT);
        return null;
    }
}
