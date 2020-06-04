package com.danawa.fastcatx.indexer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest {

    @Test
    public void testDateTimeString() {
        long timeMillis = 1591235937000L;
        String str = Utils.dateTimeString(timeMillis);
        System.out.println(str);
        assertEquals("2020-06-04 10:58:57", str);
    }
}
