package com.danawa.fastcatx.indexer;

import com.danawa.fastcatx.indexer.entity.Job;
import com.danawa.fastcatx.indexer.ingester.CSVIngester;
import com.danawa.fastcatx.indexer.ingester.JDBCIngester;
import com.danawa.fastcatx.indexer.ingester.NDJsonIngester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DynamicIndexJobRunner implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(DynamicIndexJobRunner.class);
    private enum STATUS { READY, RUNNING, SUCCESS, ERROR, STOP }
    private Job job;
    private IndexService service;
    private Ingester ingester;

    public DynamicIndexJobRunner(Job job) {
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

            //검색엔진 타입( FASTCAT, ES)
            String type = (String) payload.get("type");
            // 검색엔진 호스트
            String host = (String) payload.get("host");
            // 검색엔진 포트
            Integer port = (Integer) payload.get("port");
            // http, https
            String scheme = (String) payload.get("scheme");
            // 색인이름( , 구분자).
            String index = (String) payload.get("index");
            // 필터 클래스 이름. 패키지명까지 포함해야 한다. 예) com.danawa.fastcatx.filter.MockFilter
            String filterClassName = (String) payload.get("filterClass");
            // 벌크 사이즈
            Integer bulkSize = (Integer) payload.get("bulkSize");
            // Sleep
            Integer sleepTime = (Integer) payload.get("sleepTime");

            /**
             * file기반 인제스터 설정
             */
            //파일 경로.
            String path = (String) payload.get("path");
            // 파일 인코딩. utf-8, cp949 등..
            String encoding = (String) payload.get("encoding");
            // 테스트용도로 데이터 갯수를 제한하고 싶을때 수치.
            Integer limitSize = (Integer) payload.getOrDefault("limitSize", 0);

            ingester = new NDJsonIngester(path, encoding, 1000, limitSize);

            Ingester finalIngester = ingester;
            Filter filter = (Filter) Utils.newInstance(filterClassName);

            service = new IndexService(host, port, scheme);

            //검색엔진에 따라 서비스 구분처리
            if(type.equals("FASTCAT")) {
                service.fastcatDynamicIndex(finalIngester, index,filter,bulkSize,sleepTime);
            }else if(type.equals("ES")) {
                service.elasticDynamicIndex(finalIngester, index,filter,bulkSize,sleepTime);
            }

            service.getStorageSize(index);

            job.setStatus(STATUS.SUCCESS.name());
        } catch (StopSignalException e) {
            job.setStatus(STATUS.STOP.name());
        } catch (Throwable e) {
            job.setStatus(STATUS.ERROR.name());
            job.setError(e.getMessage());
            logger.error("error .... ", e);
        } finally {
            job.setEndTime(System.currentTimeMillis() / 1000);
        }
    }
}
