package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import org.apache.http.HttpHost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.enrich.StatsRequest;
import org.elasticsearch.client.enrich.StatsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
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
        logger.info("host : {} , port : {}, scheme : {} ", host, port,scheme);
    }

    public int getCount() {
        return count;
    }

    public boolean existsIndex(String index) throws IOException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)))) {
            GetIndexRequest request = new GetIndexRequest(index);
            return client.indices().exists(request, RequestOptions.DEFAULT);

        }catch (IOException e) {
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
            try{

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
                    if(record != null && record.size() >0) {
                        count++;

                        IndexRequest indexRequest = new IndexRequest(index).source(record, XContentType.JSON);

                        if(record.get("ID") != null) {
                            id = record.get("ID").toString();
                        }else if(record.get("id") != null) {
                            id = record.get("id").toString();
                        }

                        if(id.length() > 0) {
                            indexRequest.id(id);
                        }

                        if(pipeLine.length() > 0) {
                            indexRequest.setPipeline(pipeLine);
                        }

                        request.add(indexRequest);
                    }

                    if (count % bulkSize == 0) {
                        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                        checkResponse(bulkResponse); // 추가
                        logger.debug("bulk! {}", count);
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
            } catch(Exception e) {
                logger.info("{}",e);
                StackTraceElement[] exception = e.getStackTrace();
                logger.error("[Exception] : request id : {}" , id);
                for(StackTraceElement element : exception) {
                    e.printStackTrace();
                    logger.error("[Exception] : " + element.toString());
                    continue;
                }
            }

            long totalTime = System.currentTimeMillis() - start;
            logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
        }
    }

    public void elasticDynamicIndex(Ingester ingester, String index, Filter filter, Integer bulkSize, Integer sleepTime) throws IOException, StopSignalException, InterruptedException {
        elasticDynamicIndex(ingester,index, filter, bulkSize, sleepTime, null);
    }
    public void elasticDynamicIndex(Ingester ingester, String index, Filter filter, Integer bulkSize, Integer sleepTime, Job job) throws IOException, StopSignalException, InterruptedException {
        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme)))) {
            count = 0;
            long start = System.currentTimeMillis();

            AtomicInteger counter = new AtomicInteger();

            BulkRequest request = new BulkRequest();

            int cnt = 0;
            String[] indexArr = index.split(",");

            long time = System.nanoTime();
            ActionListener<IndexResponse> listener;

            listener = new ActionListener<IndexResponse>() {
                @Override
                public void onResponse(IndexResponse indexResponse) {
                    counter.decrementAndGet();
                }

                @Override
                public void onFailure(Exception e) {
                    counter.decrementAndGet();
                    logger.error("[DynamicIndex] : " + e.getMessage());
                }
            };


            try{
                while (ingester.hasNext()) {
                    if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                        logger.info("Stop Signal");
                        throw new StopSignalException();
                    }
//                if (counter.get() >= bulkSize) {
//                    Thread.sleep(20);
//                    continue;
//                }

                    Map<String, Object> record = ingester.next();
                    if (filter != null && record.size() > 0) {
                        record = filter.filter(record);
                    }

                    //입력된 인덱스만큼 순차적으로 색인 API 호출
                    if(record.size() > 0) {

                        request.add(new IndexRequest(indexArr[count % indexArr.length]).source(record, XContentType.JSON));
                        //IndexRequest request  = new IndexRequest(indexArr[count % indexArr.length]).source(record, XContentType.JSON);
                        //client.indexAsync(request, RequestOptions.DEFAULT, listener);
                        //counter.incrementAndGet();

//                    cnt++;
//                    if(cnt == indexArr.length) {dmd
//                        cnt = 0;
//                    }
                    }


                    count++;
                    if (count % bulkSize == 0) {
                        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                        logger.info("bulk! {}, sleep : {}", count, sleepTime);
                        request = new BulkRequest();

                        //sleepTime 만큼 대기
                        if(sleepTime != null) {
                            Thread.sleep(sleepTime);
                        }
                    }

                    if (count % 10000 == 0) {
                        //logger.info("{} DynamicIndex API Call ROWS FLUSHED! in {}ms. async_counter[{}]", count, (System.nanoTime() - time) / 1000000, counter.get());
                        logger.info("index: [{}] {} DynamicIndex API Call ROWS FLUSHED! in {}ms. SleepTime[{}]", index, count, (System.nanoTime() - time) / 1000000, sleepTime);
                    }
                }

                if (request.estimatedSizeInBytes() > 0) {
                    //나머지..
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    checkResponse(bulkResponse);
                    logger.debug("Final bulk! {}", count);
                }

            } catch (StopSignalException e) {
                throw e;
            } catch(Exception e) {
                StackTraceElement[] exception = e.getStackTrace();
                for(StackTraceElement element : exception) {
                    logger.error("[Exception] : " + element.toString());
                }
                throw e;
            }

            long totalTime = System.currentTimeMillis() - start;
            logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
        }
    }


    public void fastcatDynamicIndex(Ingester ingester, String index, Filter filter,Integer bulkSize, Integer sleepTime) throws IOException {

        WebClient webClient = WebClient.create();
        Utils utils = new Utils();

        count = 0;
        List<Map<String, Object>> indexList = new ArrayList<Map<String, Object>>();
        long start = System.currentTimeMillis();
        // IndexRequest request = new IndexRequest();

        int cnt = 0;

        String[] indexArr = index.split(",");

        long time = System.nanoTime();

        try{
            while (ingester.hasNext()) {
                count++;

                Map<String, Object> record = ingester.next();
                if (filter != null) {
                    record = filter.filter(record);
                }

                //필터적용 후 List에 적재
                if (record.size() > 0) {
                    indexList.add(record);
                }

                //bulkSize의 1/10만큼 볼륨에 동적색인
                if (count % (bulkSize/10) == 0) {

                    String uri = String.format("http://%s:%s/service/index?collectionId=%s",host,port,indexArr[cnt]);
                    //logger.info("bulk! {}", count);


                    Mono<String> result = webClient.post()
                            .uri(uri)
                            .bodyValue(utils.makeJsonData(indexList))
                            .retrieve()
                            .bodyToMono(String.class);

                    result.subscribe( s-> {
                        //logger.info("record");
                    });

                    cnt++;
                    if(cnt == indexArr.length) {
                        cnt = 0;
                    }

                    //ArrayList 초기화
                    indexList = new ArrayList<>();
                }

                //bulkSize마다 ThreadSleep
                if (count % bulkSize == 0) {
                    if(sleepTime != null) {
                        logger.info("bulk! {}, sleep : {}", count, sleepTime);
                        Thread.sleep(sleepTime);
                    }
                }

                if (count % 10000 == 0) {
                    logger.info("index: [{}] {} DynamicIndex API Call ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
                }
            }

            if (indexList.size() > 0) {

                String uri = String.format("http://%s:%s/service/index?collectionId=%s",host,port,indexArr[cnt]);
                //나머지..
                Mono<String> result = webClient.post()
                        .uri(uri)
                        .bodyValue(utils.makeJsonData(indexList))
                        .retrieve()
                        .bodyToMono(String.class);

                result.subscribe( s-> {
                    //logger.info("record");
                });
                logger.debug("Final bulk! {}", count);
            }

        }catch(Exception e) {
            StackTraceElement[] exception = e.getStackTrace();
            for(StackTraceElement element : exception) {
                logger.error("[Exception] : " + element.toString());
            }
        }



        long totalTime = System.currentTimeMillis() - start;
        logger.info("index:[{}] Flush Finished! doc[{}] elapsed[{}m]", index, count, totalTime / 1000 / 60);
    }

    class Worker implements Callable {
        private BlockingQueue queue;
        private RestHighLevelClient client;
        public Worker(BlockingQueue queue) {
            this.queue = queue;
            this.client =new RestHighLevelClient(RestClient.builder(new HttpHost(host, port, scheme))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECTION_TIMEOUT)
                            .setSocketTimeout(SOCKET_TIMEOUT))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setKeepAliveStrategy(getConnectionKeepAliveStrategy())));
        }
        @Override
        public Object call() {

            try{
                while(true) {
                    Object o = queue.take();
                    if (o instanceof String) {
                        //종료.
                        logger.info("Indexing Worker-{} got {}", Thread.currentThread().getId(), o);
                        break;
                    }
                    BulkRequest request = (BulkRequest) o;
                    BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
                    checkResponse(bulkResponse);

//                logger.debug("bulk! {}", count);
                }
            }catch (Throwable e) {
                logger.error("indexParallel : {}" , e);
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
            Worker w = new Worker(queue);
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
                    if(record.get("ID") != null) {
                        id = record.get("ID").toString();
                    }else if(record.get("id") != null) {
                        id = record.get("id").toString();
                    }

                    if(id.length() > 0) {
                        indexRequest.id(id);
                    }

                    if(pipeLine.length() > 0) {
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
            if(!executorService.isShutdown()){
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
