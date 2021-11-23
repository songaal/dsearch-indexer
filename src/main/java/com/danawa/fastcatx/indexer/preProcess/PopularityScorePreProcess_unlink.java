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

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import com.danawa.fastcatx.indexer.IndexJobRunner;

public class PopularityScorePreProcess_unlink implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(PopularityScorePreProcess_unlink.class);
    private Job job;
    private Map<String, Object> payload;
    private RestClientBuilder builder;

    private String filePath;

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


        /**
         * 제휴상품 인기점수 전처리
         *  1) 쿼리를 통해 제휴상품별 인기점수 조회
         *  2) 전날 생성된 파일과 비교하여 초기화할 상품을 필터링한다.
         *        - 파일이 없을 경우 select 된 결과를 바로 es에 업데이트 후 파일 생성
         *  3) 필터링된 상품은 금일 조회되지 않는 상품이므로 0점으로 업데이트 한다
         *  4) 조회된 결과로 상품점수를 업데이트 한 후 파일로 생성한다.
         *  5) 매 스케쥴마다 위 내용을 반복
         */

        int threadSize = Integer.parseInt(payload.get("threadSize").toString());
        String host = payload.get("host").toString();
        Integer port = (int) payload.get("port");
        Integer bulkSize = (int) payload.get("bulkSize");
        String scheme = payload.get("scheme").toString();
        String index = payload.get("index").toString();
        String username = payload.get("username").toString();
        String password = payload.get("password").toString();
        filePath = payload.get("path").toString();


        /**
         *  관리도구에서 index를 지정하여도 해당 컬랙션에 해당하는 index로 dserch-indexer를 호출하여 하드코딩
         *  제휴 상품 인덱스 네임을 지정
         */
        index = "s-prod";
        IndexService service = new IndexService(host,port,scheme,username,password);
        builder = service.getRestClientBuilder();

        /**
         * 기존에 제휴상품 인기점수 파일이 있을 경우
         * 파일을 읽어 어제 배치로 수행된 검색상품들의 점수를 0점으로 초기화 한다.
         */
        File popularityScoreFile = new File(filePath+"scoreData.txt");
        if(popularityScoreFile.exists()) {
            logger.info("전날 제휴상품 인기점수 데이터 로드");

            //어제와 오늘 모두 조회된 검색상품의 경우 초기화 할 필요가 없으므로 select한 결과와 어제 파일에서 읽어온 상품을 비교
            //초기화 대상만 map에 적재한다.
            Map<String,Integer> map = loadScoreData(popularityScoreFile, mapPopularityScore);
            logger.info("init Map size {}", map.size());
            logger.info("제휴상품 인기점수 초기화 시작");
            scoreDataBulkUpdate("init",map,threadSize,index,bulkSize);
            logger.info("제휴상품 인기점수 초기화 종료");
        }

        logger.info("제휴상품 인기점수 갱신 시작");
        scoreDataBulkUpdate("",mapPopularityScore,threadSize,index,bulkSize);
        logger.info("제휴상품 인기점수 갱신 종료");

        logger.info("제휴상품 인기점수 파일생성 시작");
        writeScoreData(mapPopularityScore, popularityScoreFile);
        logger.info("제휴상품 인기점수 파일생성 종료");

//        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);
//
//        BlockingQueue queue = new LinkedBlockingQueue(threadSize * 10);
//        //여러 쓰레드가 작업큐를 공유한다.
//        List<Future> list = new ArrayList<>();
//        for (int i = 0; i < threadSize; i++) {
//            PopularityScorePreProcess_unlink.Worker w = new Worker(queue, index, job);
//            list.add(executorService.submit(w));
//        }
//
//        int count = 0;
//
//        long start = System.currentTimeMillis();
//        BulkRequest request = new BulkRequest();
//        long time = System.nanoTime();
//        try {
//            for(Map.Entry scoreSet : mapPopularityScore.entrySet()) {
//                if (job != null && job.getStopSignal() != null && job.getStopSignal()) {
//                    logger.info("Stop Signal");
//                    throw new StopSignalException();
//                }
//
//                Map doc = new HashMap();
//                doc.put("popularityScore", scoreSet.getValue());
//
//                UpdateRequest updateRequest = new UpdateRequest().index(index);
//                updateRequest.id(scoreSet.getKey().toString()).doc(doc);
//                request.add(updateRequest);
//
//                count++;
//
//                if (count % bulkSize == 0) {
//                    queue.put(request);
//                    request = new BulkRequest();
//                }
//
//                if (count % 100000 == 0) {
//                    logger.info("index: [{}] {} ROWS FLUSHED! in {}ms", index, count, (System.nanoTime() - time) / 1000000);
//                }
//            }
//
//            if (request.estimatedSizeInBytes() > 0) {
//                //나머지..
//                queue.put(request);
//                logger.info("Final bulk! {}", count);
//            }
//
//
//        } catch (InterruptedException e) {
//            logger.error("interrupted! ", e);
//        } catch (Exception e) {
//            logger.error("[Exception] ", e);
//        } finally {
//            try {
//                for (int i = 0; i < list.size(); i++) {
//                    // 쓰레드 갯수만큼 종료시그널 전달.
//                    queue.put("<END>");
//                }
//
//            } catch (InterruptedException e) {
//                logger.error("", e);
//                //ignore
//            }
//
//            try {
//                for (int i = 0; i < list.size(); i++) {
//                    Future f = list.get(i);
//                    f.get();
//                }
//            } catch (Exception e) {
//                logger.error("", e);
//                //ignore
//            }
//
//            // 큐 안의 내용 제거
//            logger.info("queue clear");
//            queue.clear();
//
//            // 쓰레드 종료
//            logger.info("{} thread shutdown", index);
//            executorService.shutdown();
//
//            // 만약, 쓰레드가 정상적으로 종료 되지 않는다면,
//            if (!executorService.isShutdown()) {
//                // 쓰레드 강제 종료
//                logger.info("{} thread shutdown now!", index);
//                executorService.shutdownNow();
//            }
//        }
//
//        long totalTime = System.currentTimeMillis() - start;
//
//        logger.info("인기점수 전처리 완료하였습니다. 총 {} - 소요시간 {}m ", count, totalTime / 1000/ 60);
        job.setStatus(IndexJobRunner.STATUS.SUCCESS.name());

    }

    void scoreDataBulkUpdate(String type, Map<String, Integer> mapPopularityScore, int threadSize, String index, int bulkSize) {

        ExecutorService executorService = Executors.newFixedThreadPool(threadSize);

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
                /**
                 * 초기화면 0점으로 업데이트
                 */
                if(type.equals("init")) {
                    doc.put("popularityScore", 0);
                }else{
                    doc.put("popularityScore", scoreSet.getValue());
                }

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

        if (type.equals("init")) {
            logger.info("인기점수 초기화 완료하였습니다. 총 {} - 소요시간 {}m ", count, totalTime / 1000/ 60);
        }else{
            logger.info("인기점수 전처리 완료하였습니다. 총 {} - 소요시간 {}m ", count, totalTime / 1000/ 60);
        }


    }

    public Map<String, Integer> loadScoreData(File file, Map<String,Integer> mapPopularityScore) {

        try {
            logger.info("file : {} - {}", file.getPath(), file.getName());
            BufferedReader br = new BufferedReader(new FileReader(file));
            Map<String, Integer> scoreMap = new HashMap<>();

            int exsistCount = 0;
            String rline = null;
            while((rline = br.readLine()) != null) {

                //ob[0] = 검색상품 키
                //ob[1] = 제휴상품 인기점수
                String[] scoreArr = rline.split("\t");

                //전날 데이터와 금일 데이터에 모두 있는 상품은 초기화 할 필요가 없으므로 scoreMap에 담지 않는다.
                if(scoreArr.length == 2 && mapPopularityScore.get(scoreArr[0]) == null) {
                    scoreMap.put(scoreArr[0], Integer.parseInt(scoreArr[1]));
                }else if (mapPopularityScore.get(scoreArr[0]) != null) {
                    exsistCount++;
                }
            }
            br.close();
            logger.info("제휴상품이 어제와 동일하게 존재하는 사움 개수 : {}", exsistCount);
            return scoreMap;

        } catch (FileNotFoundException e) {
            logger.error("file not found : {}" ,e.getMessage());
        } catch (IOException e) {
            logger.error("load score file error : {}" ,e.getMessage());
        }

        return null;
    }

    void writeScoreData(Map<String,Integer> mapPopularityScore, File file) {

        try {
            /**
             *  기존파일 있을 경우 지우고 새로 생성
             */
            if(file.exists()) {
                file.delete();
            }
            file.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(file));

            logger.info("scoreMap Size : {}", mapPopularityScore.size());
            for(Map.Entry entry : mapPopularityScore.entrySet()) {
                bw.write(entry.getKey()+"\t"+entry.getValue());
                bw.newLine();
            }

            bw.close();

        } catch (IOException e) {
            logger.error("write score file error : {}" ,e.getMessage());
        }
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

