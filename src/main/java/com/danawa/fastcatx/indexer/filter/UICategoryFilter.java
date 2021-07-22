package com.danawa.fastcatx.indexer.filter;

import com.danawa.convertcategory.CategoryScheduler;
import com.danawa.convertcategory.entity.CategoryMappingModel;
import com.danawa.convertcategory.entity.UICategory;
import com.danawa.fastcatx.indexer.Filter;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

public class UICategoryFilter implements Filter {
    private CategoryScheduler categoryScheduler;
    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public void init(Map<String, Object> payload) {
        String categoryMappingUrl = (String) payload.getOrDefault("categoryMappingUrl", "");
        CategoryMappingModel mappingModel = restTemplate.getForObject(categoryMappingUrl, CategoryMappingModel.class);
        categoryScheduler = new CategoryScheduler(mappingModel);
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> item) {
        // 물리카테고리 코드
        List<Integer> categoryCode = new ArrayList<>();
        for (int i = 4; i >= 1; i--) {
            categoryCode.add((Integer) item.getOrDefault("categoryCode" + i, 0));
        }
        // 옵션들
        Integer makerCode = (Integer) item.getOrDefault("makerCode", null);
        Integer brandCode = (Integer) item.getOrDefault("brandCode", null);
        // 123,123,123
        List<String> nAttributeValueSeqStrList = Arrays.asList(((String) item.getOrDefault("nAttributeValueSeq", "")).split(","));
        List<Integer> nAttributeValueSeqList = new ArrayList<>();
        for (String attr : nAttributeValueSeqStrList) {
            nAttributeValueSeqList.add(Integer.parseInt(attr));
        }

        // 찾아본다.
        Set<Map<String, Object>> mapping = categoryScheduler.findUICategoryList(categoryCode, nAttributeValueSeqList, makerCode, brandCode);
        /*  구조
        * [{
            "brandOption": [],
            "makerOption": [],
            "attributeValueOption": [],
            "categoryCode": [
                842,
                58439,
                58442,
                0
            ],
            "uiCategory": {
                "code": 37598,
                "name": [
                    "태블릿/모바일/디카",
                    "촬영용품",
                    "플래시/조명/배경",
                    "플래시/라이트"
                ]
            },
            "categoryName": [
                "",
                "카메라/캠코더용품",
                "플래시/라이트",
                "디카"
            ]
        }]
        * */
        if (mapping != null && mapping.size() > 0) {
            Set<List<String>> nameList = mapping.stream().map(stringObjectMap -> ((UICategory)stringObjectMap.get("uiCategory")).getName()).collect(Collectors.toSet());
            Set<Integer> codeList = mapping.stream().map(stringObjectMap -> ((UICategory)stringObjectMap.get("uiCategory")).getCode()).collect(Collectors.toSet());
            item.put("uiCategoryCode", codeList);
            item.put("uiCategoryName", nameList);
        } else {
            item.put("uiCategoryCode", new HashSet<>());
            item.put("uiCategoryName", new HashSet<>());
        }
        return item;
    }


}
