package com.danawa.fastcatx.indexer;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.TimeZone;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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




    public <E> String makeJsonData(ArrayList<E> list){
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
}
