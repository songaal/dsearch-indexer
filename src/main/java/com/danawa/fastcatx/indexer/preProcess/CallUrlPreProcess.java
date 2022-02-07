package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.IndexJobRunner;
import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CallUrlPreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(CallUrlPreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    public CallUrlPreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("UPDATE URL 호출 시작합니다");
        // 업데이트 URL
        String updateURL = (String) payload.getOrDefault("updateURL", "");
        // 업데이트 호출 시 파라미터
        String updateURLParams = (String) payload.getOrDefault("JSONParams", "");
        // 완료여부 조회 URL
        String statusURL = (String) payload.getOrDefault("statusURL", "");

        // UPDATE 요청
        requestUrl("PUT", updateURL, updateURLParams);

        // 완료여부 조회 로직
        while(true){
            // 1분 간격 체크
            Thread.sleep(60000);
            StringBuilder response = requestUrl("GET", statusURL, "");

            // 모든 큐 소진시 STOP이 됨
            if("STOP".equals(response.toString())){
                break;
            }
        }

        logger.info("작업 완료 되었습니다.");
        job.setStatus(IndexJobRunner.STATUS.SUCCESS.name());
    }

    /*
    * URL 호출 메소드
    * 호출 타입, 주소, 파라미터(JSON)
    */
    public static StringBuilder requestUrl(String type, String reqUrl, String param) throws IOException {
        // URL 초기화
        URL url = new URL(reqUrl);

        // 연결 초기화
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(type);
        con.setRequestProperty("Content-Type", "application/json; utf-8");
        con.setRequestProperty("Accept", "application/json");
        // 출력 콘텐츠 허용
        con.setDoOutput(true);

        // 요청 본문 만들기
        if(!"".equals(param)){
            try(OutputStream os = con.getOutputStream()) {
                byte[] input = param.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
        }

        StringBuilder response = new StringBuilder();

        // 입력 스트림에서 응답 읽기
        try(BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            logger.info("response : {}", response);
        }

        return response;
    }
} 