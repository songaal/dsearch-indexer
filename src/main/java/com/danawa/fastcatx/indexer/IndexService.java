package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
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
import org.elasticsearch.client.*;
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
}
