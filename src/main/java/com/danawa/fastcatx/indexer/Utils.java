package com.danawa.fastcatx.indexer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.Gson;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final Pattern ptnHead = Pattern.compile("\\x5b[%]([a-zA-Z0-9_-]+)[%]\\x5d");

    private Gson gson =new Gson();

    public static Object newInstance(String className) {
        if (className == null) {
            return null;
        }
        try {
            Class<?> clazz = Class.forName(className);
            Constructor constructor = clazz.getConstructor();
            return constructor.newInstance();
        } catch (Exception e) {
            logger.error("error newInstance.", e);
            return null;
        }

    }

    public static String dateTimeString(long timeMillis) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeMillis),
                        TimeZone.getDefault().toZoneId());
        return formatter.format(localDateTime);
    }




    public <E> String makeJsonData(List<Map<String, Object>> list){
        int listSize = list.size();
        //logger.debug("json 생성 시작");
        StringBuilder sb = new StringBuilder();
        for(int i=0; i< listSize; i++){
            sb.append(gson.toJson(list.get(i)));
            sb.append("\n");
        }
        String result = sb.toString();
        //logger.debug("json 생성 종료");

        if(result.length() > 0){
            //logger.debug("\n" + result);
        }
        //logger.info("transper data cnt : {}",listSize);
        return result;
    }


    //임시 : KONAN 형식 데이터를 NDJSON으로 변환
    public static String convertKonanToNdJson(String line) throws IOException {

        boolean isSourceFile = false;
        JsonGenerator generator = null;
        StringWriter writer = null;
        String ndjsonString = "";

        Matcher mat = ptnHead.matcher(line);
        String key = null;
        int offset = 0;

        while (mat.find()) {

            if (!isSourceFile) {
                //row 처음에 한번만 실행.
                writer = new StringWriter();
                generator = new JsonFactory().createGenerator(writer);

                generator.writeStartObject();
            }
            isSourceFile = true;
            if (key != null) {
                String value = line.substring(offset, mat.start()).trim();
                if (key.equals("")) {
                    logger.error("ERROR >> {}:{}", key, value);
                }
                logger.debug("{} > {}", key, value);
                generator.writeStringField(key, value);
            }
            key = mat.group(1);
            offset = mat.end();
        }
        if (isSourceFile) {
            String value = line.substring(offset);
            generator.writeStringField(key, value);
            generator.writeEndObject();
            generator.close();

            ndjsonString = writer.toString();

        }

        return ndjsonString;

    }

    public static boolean checkFile(String path, String filename){
        File f = new File(path + "/" + filename);
        return f.exists();
    }


    public static HttpComponentsClientHttpRequestFactory getRequestFactory() {
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
        try {
            TrustStrategy trustStrategy = (X509Certificate[] chain, String authType) -> true;
            SSLContext sslContext = SSLContexts
                    .custom()
                    .loadTrustMaterial(null, trustStrategy)
                    .build();
            SSLConnectionSocketFactory csf = new SSLConnectionSocketFactory(sslContext);
            CloseableHttpClient client = HttpClients
                    .custom()
                    .setSSLSocketFactory(csf)
                    .build();
            requestFactory.setHttpClient(client);
        } catch (Exception e) {
            logger.error("", e);
        }
        return requestFactory;
    }

    public static String calcSpendTime(long start, long end) {
        String content = "";

        long time = end - start; // 총 수행시간
        double totalsecond = time / (double) 1000; // 총 수행 초 (ex : 333초)
        int minute = (int) totalsecond / 60; // 분 (5)
        int second = (int) totalsecond % 60; // 초(33)

        content = " spend time : " + minute + " min" + " " + second + " sec";

        return content;
    }

    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {}
    }


    public static String[] decodeMenu(String menuStr) {

        List<String> menu2List = new ArrayList<>();
        List<String> menu3List = new ArrayList<>();
        List<String> menu4List = new ArrayList<>();
        List<String> menu5List = new ArrayList<>();
        String menu2 = "";
        String menu3 = "";
        String menu4 = "";
        String menu5 = "";

        // String menuStr = "1>20>35>160,1>20>36>170,1>27>36>180,1>27>36>184,1>27";

        String[] menuList = menuStr.split(",");

        for (String list : menuList) {

            String[] menus = list.split(">");
            switch (menus.length) {
                case 5:
                    menu2List.add(menus[1]);
                    menu3List.add(menus[2]);
                    menu4List.add(menus[3]);
                    menu5List.add(menus[4]);
                    break;
                case 4:
                    menu2List.add(menus[1]);
                    menu3List.add(menus[2]);
                    menu4List.add(menus[3]);
                    break;
                case 3:
                    menu2List.add(menus[1]);
                    menu3List.add(menus[2]);
                    break;
                case 2:
                    menu2List.add(menus[1]);
                    break;
            }
        }
        menu2 = getDistinctMenu(menu2List);
        menu3 = getDistinctMenu(menu3List);
        menu4 = getDistinctMenu(menu4List);
        menu5 = getDistinctMenu(menu5List);

        return new String[]{ menu2, menu3, menu4, menu5 };
    }

    public static String getDistinctMenu(List<String> menuList) {

        if (menuList != null) {
            HashSet set = new HashSet(menuList);

            String menu = set.toString().replaceAll("\\[,|\\[|\\]|\\,]", "").replace(" ", "");

            // DB 필드 사이즈 크기가 넘어설 경우에 자름
            if (menu.length() > 100) {
                menu = menu.substring(0, 100);

                int idx = menu.lastIndexOf(",");
                menu = menu.substring(0, idx);
            }
            return menu;

        }
        return "";
    }


}
