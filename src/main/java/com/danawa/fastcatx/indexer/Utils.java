package com.danawa.fastcatx.indexer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class Utils {
    private static Logger logger = LoggerFactory.getLogger(Utils.class);

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
}
