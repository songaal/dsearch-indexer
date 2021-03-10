package com.danawa.fastcatx.indexer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
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
}
