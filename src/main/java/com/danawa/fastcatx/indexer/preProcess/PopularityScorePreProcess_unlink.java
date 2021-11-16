package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.IndexService;
import com.danawa.fastcatx.indexer.StopSignalException;
import com.danawa.fastcatx.indexer.entity.Job;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

public class PopularityScorePreProcess_unlink implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(PopularityScorePreProcess_unlink.class);
    private Job job;
    private Map<String, Object> payload;
    private RestClientBuilder builder;

    public PopularityScorePreProcess_unlink(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("인기점수 전처리를 시작합니다.");
        // 파라미터
        String logDBDriver = (String) payload.getOrDefault("logDBDriver", "");
        String logDBAddress = (String) payload.getOrDefault("logDBAddress", "");
        String logDBUsername = (String) payload.getOrDefault("logDBUsername", "");
        String logDBPassword = (String) payload.getOrDefault("logDBPassword", "");
//        remakePopularityScore_new.sql
        String selectSql = (String)  payload.getOrDefault("selectSql", "");

        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn("logDB", logDBDriver, logDBAddress, logDBUsername, logDBPassword);
        int totalcount = 0;
        // 1. 검색상품 클릭 로그 데이터 가져오기 - mysql
        // 2. 데이터 ES Bulk Update
        Map<String, Integer> mapPopularityScore = new HashMap<>();

        try (Connection connection = databaseConnector.getConn("logDB")) {
            logger.debug("mysql tPVShopItemBuyClick 수집 시작.");

            long queryStartTime = System.currentTimeMillis(); // 쿼리 시작 시간
            PreparedStatement preparedStatement = connection.prepareStatement(selectSql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet resultSet = preparedStatement.executeQuery(); // 쿼리문 전송
            long queryEndTime = System.currentTimeMillis(); // 쿼리 종료 시간
            logger.debug("mysql tPVShopItemBuyClick 쿼리 시간 : " + (queryEndTime - queryStartTime));

            double queryTime = (queryEndTime - queryStartTime) / (double) 1000;
            int minute = (int) queryTime / 60;
            int second = (int) queryTime % 60;
            logger.debug("mysql tPVShopItemBuyClick 수집 시간 : " + minute + " min" + " " + second + " sec");

            resultSet.last(); // 커서를 맨아래로
            int recodeCount = resultSet.getRow(); // 데이터 개수 체크
            resultSet.beforeFirst(); // 커서를 맨위로
            while (resultSet.next()) {
                String cmpny_c = resultSet.getString("sShopId");
                String link_prod_c = resultSet.getString("sShopItemId");
                int pcScore = resultSet.getInt("nPcNormalCount");
                int mobileScore = resultSet.getInt("nMobileWebNormalCount");
                int score = pcScore + mobileScore;
                if (cmpny_c.length() > 6) {
                    cmpny_c = cmpny_c.substring(0, 6);
                }
                boolean pattern_cmpny_c = Pattern.matches("^[a-zA-Z0-9]*$", cmpny_c);
                boolean pattern_link_prod_c = Pattern.matches("^[a-zA-Z0-9]*$", link_prod_c);

                if (pattern_cmpny_c && pattern_link_prod_c) {
                    String key = cmpny_c + "_" + link_prod_c;
                    Integer tempScore = mapPopularityScore.get(key);
                    if (tempScore == null) {
                        mapPopularityScore.put(key, score);
                    } else if (tempScore != null) {
                        mapPopularityScore.put(key, score + tempScore);
                    }
                }
            }
            logger.info("제휴상품 인기점수 갱신 전 개수:" + recodeCount);
            logger.info("제휴상품 인기점수 갱신 후 개수:" + mapPopularityScore.size());
            logger.debug("mysql tPVShopItemBuyClick 수집 종료.");
        } catch (Exception e) {
            logger.error("", e);
//            TODO 알림처리
            throw e;
        }


        int threadSize = Integer.parseInt(payload.get("threadSize").toString());
        String host = payload.get("host").toString();
        Integer port = (int) payload.get("port");
        Integer bulkSize = (int) payload.get("bulkSize");
        String scheme = payload.get("scheme").toString();
        String index = payload.get("index").toString();
        String username = payload.get("username").toString();
        String password = payload.get("password").toString();



        //임시..
        index = "s-prod";

        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
        IndexService service = new IndexService(host,port,scheme,username,password);
        builder = service.getRestClientBuilder();

        BlockingQueue queue = new LinkedBlockingQueue(threadSize * 10);
        //여러 쓰레드가 작업큐를 공유한다.
        List<Future> list = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            PopularityScorePreProcess_unlink.Worker w = new Worker(queue, index, job);
            list.add(executorService.submit(w));
        }

        int count = 0;

        long start = System.currentTimeMillis();
        BulkRequest request = new BulkRequest();
        long time = System.nanoTime();
        try {
            for(Map.Entry scoreSet : mapPopularityScore.entrySet()) {
                if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
                    logger.info("Stop Signal");
                    throw new StopSignalException();
                }

                Map doc = new HashMap();
                doc.put("popularityScore", scoreSet.getValue());

                UpdateRequest updateRequest = new UpdateRequest().index(index);
                updateRequest.id(scoreSet.getKey().toString()).doc(doc);
                request.add(updateRequest);

                count++;

                if (count % bulkSize == 0) {
                    queue.put(request);
                    request = new BulkRequest();
                }

                if (count % 100000 == 0) {
                    logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
                }
            }

            if (request.estimatedSizeInBytes() > 0) {
                //나머지..
                queue.put(request);
                logger.info("Final bulk! {}", count);
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

        logger.info("인기점수 전처리 완료하였습니다. 총 {} - 소요시간 {}m ", count, totalTime / 1000/ 60);

//        try{
//            //String host, Integer port, String scheme, String esUsername, String esPassword
//            String host = payload.get("host").toString();
//            Integer port = (int) payload.get("port");
//            String scheme = payload.get("scheme").toString();
//           // String esUsername = payload.get("esUsername").toString();
//           // String esPassword = payload.get("esPassword").toString();
//
//            RestHighLevelClient client = new IndexService(host,port,scheme).getClient();
//
//            BulkRequest bulkRequest = new BulkRequest();
//
//            int count = 0;
//            for(Map.Entry scoreSet : mapPopularityScore.entrySet()) {
//
//                UpdateRequest request = new UpdateRequest().index(payload.get("index").toString());
//                if(count == 0) {
//                    logger.info("{}", scoreSet.getKey());
//                    logger.info("{}", scoreSet.getValue());
//                }
//                Map doc = new HashMap();
//                doc.put("popularityScore", scoreSet.getValue());
//                request.id(scoreSet.getKey().toString());
//                request.doc(doc);
//                bulkRequest.add(request);
//                totalcount++;
//                count++;
//
//                if(count % 1000 == 0) {
//                    logger.info("count : {}" ,count);
//                    BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//                    if(!response.hasFailures()) {
//                        logger.info("update {} - {}ms", count, response.getTook());
//                        bulkRequest = new BulkRequest();
//                        count = 0;
//                    }else{
//                        logger.error("error : {}", response.status());
//                    }
//                }
//            }
//
//            if(count > 0) {
//                BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//                if(!response.hasFailures()) {
//                    logger.info("update {} - {}ms", count, response.getTook());
//                }else{
//                    logger.error("error : {}",response.buildFailureMessage());
//                }
//            }
//
//        }catch(Exception e) {
//            logger.error("", e);
//            throw e;
//        }


    }

    class Worker implements Callable {
        private BlockingQueue queue;
        private RestHighLevelClient client;
        private int sleepTime = 1000;
        private boolean isStop = false;
        private String index;
        private Job job;
        private int requestCount;
        private int docMissingCount = 0;

        public Worker(BlockingQueue queue, String index, Job job) {
            this.queue = queue;
            this.index = index;
            this.client = new RestHighLevelClient(builder);
            this.job = job;
        }

        private void retry(BulkRequest bulkRequest) {
            // 동기 방식
            try {

                BulkResponse bulkResponse;
                // 문서 손실 방지. es rejected 에러시 무한 색인 요청
                while (true) {
                    bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                    BulkRequest retryBulkRequest = new BulkRequest();
                    requestCount = bulkResponse.getItems().length;
                    if (bulkResponse.hasFailures()) {

                        // bulkResponse에 에러가 있다면
                        // retry 1회 시도
                        BulkItemResponse[] bulkItemResponses = bulkResponse.getItems();
                        List<DocWriteRequest<?>> requests = bulkRequest.requests();
                        for (int i = 0; i < bulkItemResponses.length; i++) {
                            BulkItemResponse bulkItemResponse = bulkItemResponses[i];

                            if (bulkItemResponse.isFailed()) {
                                BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                                // write queue reject 이슈 코드 = ( 429 )
                                if (failure.getStatus() == RestStatus.fromCode(429)) {
//                                logger.error("write queue rejected!! >> {}", failure);

                                    // retry bulk request에 추가
                                    // bulkRequest에 대한 response의 순서가 동일한 샤드에 있다면 보장.
                                    // https://discuss.elastic.co/t/is-the-execution-order-guaranteed-in-a-single-bulk-request/100412
                                    retryBulkRequest.add(requests.get(i));
                                } else {
                                    if (failure.getMessage().contains("document missing")) {
                                        docMissingCount++;
                                    }
                                    //logger.error("Doc index error >> {}", failure);
                                }
                            }
                        }

                    }

                    if (retryBulkRequest.requests().size() == 0) {
                        break;
                    } else {
                        logger.info("Retry Bulk Size {}", bulkRequest.requests().size());
                        bulkRequest = retryBulkRequest;
                    }

                    if (job.getStopSignal()) {
                        throw new StopSignalException();
                    }
                }

                logger.info("Bulk Success : index:{} - request:{} - doc mssing:{} - elapsed:{}", index, requestCount, docMissingCount , bulkResponse.getTook());
                docMissingCount=0;
            } catch (Exception e) {
                logger.error("retry", e);
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
                    retry(request);
                    logger.debug("remained queue : {}", queue.size());

                }
            } catch (Throwable e) {
                logger.error("indexParallel : {}", e);
            }

            return null;
        }
    }
}

