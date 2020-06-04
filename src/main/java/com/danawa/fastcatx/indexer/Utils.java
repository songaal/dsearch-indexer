package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
}
