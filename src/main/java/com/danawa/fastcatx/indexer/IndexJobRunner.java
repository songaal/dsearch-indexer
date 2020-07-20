package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class IndexJobRunner implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(IndexJobRunner.class);
    private enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }
    private Job job;
    private IndexService service;
    private Ingester ingester;

    public IndexJobRunner(Job job) {
        this.job = job;
        job.setStatus(STATUS.READY.name());
        job.setStartTime(System.currentTimeMillis() / 1000);
        service = null;
        ingester = null;
    }

    @Override
    public void run() {
        try {
            job.setStatus(STATUS.RUNNING.name());
            Map<String, Object> payload = job.getRequest();
            logger.info("Started Indexing Job Runner");
            // 공통
            // ES 호스트
            String host = (String) payload.get("host");
            // ES 포트
            Integer port = (Integer) payload.get("port");
            // http, https
            String scheme = (String) payload.get("scheme");
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

            logger.debug("index: {}", index);
            /**
             * file기반 인제스터 설정
             */
            //파일 경로.
            String path = (String) payload.get("path");
            // 파일 인코딩. utf-8, cp949 등..
            String encoding = (String) payload.get("encoding");
            // 테스트용도로 데이터 갯수를 제한하고 싶을때 수치.
            Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

            if (type.equals("ndjson")) {
                ingester = new NDJsonIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("csv")) {
                ingester = new CSVIngester(path, encoding, 1000, limitSize);
            } else if (type.equals("jdbc")) {
                String driverClassName = (String) payload.get("driverClassName");
                String url = (String) payload.get("url");
                String user = (String) payload.get("user");
                String password = (String) payload.get("password");
                String dataSQL = (String) payload.get("dataSQL");
                Integer fetchSize = (Integer) payload.get("fetchSize");
                Integer maxRows = (Integer) payload.getOrDefault("maxRows", 0);
                Boolean useBlobFile = (Boolean) payload.getOrDefault("useBlobFile", false);
                ingester = new JDBCIngester(driverClassName, url, user, password, dataSQL, bulkSize, fetchSize, maxRows, useBlobFile);
            }

            Ingester finalIngester = ingester;
            Filter filter = (Filter) Utils.newInstance(filterClassName);
            service = new IndexService(host, port, scheme);

            // 인덱스를 초기화하고 0건부터 색인이라면.
            if (reset) {
                if (service.existsIndex(index)) {
                    service.deleteIndex(index);
                }
            }

            if (threadSize > 1) {
                service.indexParallel(finalIngester, index, bulkSize, filter, threadSize, job);
            } else {
                service.index(finalIngester, index, bulkSize, filter, job);
            }

            job.setStatus(STATUS.SUCCESS.name());
        } catch (StopSignalException e) {
            job.setStatus(STATUS.STOP.name());
        } catch (Exception e) {
            job.setStatus(STATUS.ERROR.name());
            job.setError(e.getMessage());
            logger.error("error .... ", e);
        } finally {
            job.setEndTime(System.currentTimeMillis() / 1000);
        }
    }
}
