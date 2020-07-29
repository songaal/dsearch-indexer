package com.danawa.fastcatx.indexer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest {

    @Test
    public void testDateTimeString() {
        long timeMillis = 1591235937000L;
        String str = Utils.dateTimeString(timeMillis);
        System.out.println(str);
        assertEquals("2020-06-04 10:58:57", str);
    }




    @Test
    public void convertKonanToJson() throws IOException {
        String row = "[%PRODUCTCODE%] [%SHOPCODE%]TH201 [%SHOPPRODUCTCODE%]6666983 [%PRODUCTNAME%][니콘코리아정품] D300 +니콘18-135mm/실속사은품증정 P1 [%PRODUCTMAKER%]니콘 [%PRODUCTIMAGEURL%]http://i.011st.com/t/300/pd/17/6/6/6/9/8/3/6666983_B.jpg [%PCPRICE%]2486400 [%MOBILEPRICE%]2486400 [%SHOPCOUPON%]103600 [%SHOPGIFT%] [%DATASTAT%]F [%CATEGORYCODE1%]842 [%CATEGORYCODE2%]843 [%CATEGORYCODE3%]57284 [%CATEGORYCODE4%]0 [%REGISTERDATE%]20190217 [%DELIVERYPRICE%]0 [%SIMPRODMEMBERCNT%]0 [%POPULARITYSCORE%]0 [%GROUPSEQ%]5 [%ADDDESCRIPTION%]";
        String jsonRow = "{\"PRODUCTCODE\":\"\",\"SHOPCODE\":\"TH201\",\"SHOPPRODUCTCODE\":\"6666983\",\"PRODUCTNAME\":\"[니콘코리아정품] D300 +니콘18-135mm/실속사은품증정 P1\",\"PRODUCTMAKER\":\"니콘\",\"PRODUCTIMAGEURL\":\"http://i.011st.com/t/300/pd/17/6/6/6/9/8/3/6666983_B.jpg\",\"PCPRICE\":\"2486400\",\"MOBILEPRICE\":\"2486400\",\"SHOPCOUPON\":\"103600\",\"SHOPGIFT\":\"\",\"DATASTAT\":\"F\",\"CATEGORYCODE1\":\"842\",\"CATEGORYCODE2\":\"843\",\"CATEGORYCODE3\":\"57284\",\"CATEGORYCODE4\":\"0\",\"REGISTERDATE\":\"20190217\",\"DELIVERYPRICE\":\"0\",\"SIMPRODMEMBERCNT\":\"0\",\"POPULARITYSCORE\":\"0\",\"GROUPSEQ\":\"5\",\"ADDDESCRIPTION\":\"\"}";
        //System.out.println(Utils.convertKonanToNdJson(row));
        assertEquals(jsonRow, Utils.convertKonanToNdJson(row));

    }
}
