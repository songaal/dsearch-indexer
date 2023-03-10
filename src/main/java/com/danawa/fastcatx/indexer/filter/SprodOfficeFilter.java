package com.danawa.fastcatx.indexer.filter;

import com.danawa.fastcatx.indexer.Filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class SprodOfficeFilter implements Filter {
    HashSet set = new HashSet();
    public SprodOfficeFilter() {

        set.add("shopCode");
        set.add("shopProductCode");
        set.add("groupSeq");
        set.add("categoryCode1");
        set.add("categoryCode2");
        set.add("categoryCode3");
        set.add("categoryCode4");
        set.add("productName");
        set.add("pcPrice");
        set.add("deliveryPrice");
        set.add("registerDate");
        set.add("modifyDate");
        set.add("productMaker");
        set.add("mobilePrice");
        //set.add("productImageUrl");
        //set.add("popularityScore");

        set.add("dataStat");
        set.add("model");
        set.add("orderDateYN");
        set.add("bestGoodsSort");
        set.add("cmpnyCateCode");

        set.add("minabYN");
        set.add("orderCount");
        set.add("totalOrderPrice");
        set.add("promotionName");

        set.add("registerDateTime");
        set.add("modifyDateTime");

    };

    @Override
    public Map<String, Object> filter(Map<String, Object> item) {

        HashMap<String,Object> indexHash = new HashMap<>();

        /**
         *  검색ES / 오피스ES에 필요한 필드만 색인하기 위해 필터
         */
        for(Map.Entry entry: item.entrySet()) {

            if(set.contains(entry.getKey())) {
                indexHash.put(entry.getKey().toString(), entry.getValue());
            }

        }
        return indexHash;
    }
}
