package com.danawa.fastcatx.indexer.filter;

import com.danawa.convertcategory.CategoryScheduler;
import com.danawa.convertcategory.entity.CategoryMappingModel;
import com.danawa.convertcategory.entity.UICategory;
import com.danawa.fastcatx.indexer.Filter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class UICategoryFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(UICategoryFilter.class);
    private CategoryScheduler categoryScheduler;
    private RestTemplate restTemplate = new RestTemplate();

    @Override
    public void init(Map<String, Object> payload) {
        String categoryMappingUrl = (String) payload.getOrDefault("categoryMappingUrl", "");
        logger.info("UI카테고리 맵핑 조회 URL: {}", categoryMappingUrl);
        String body = restTemplate.getForObject(categoryMappingUrl, String.class);
        Type type = new TypeToken<CategoryMappingModel<String, List<CategoryMappingModel.Mapping>>>(){}.getType();
        CategoryMappingModel<String, List<CategoryMappingModel.Mapping>> mappingModel = new Gson().fromJson(body, type);
        if (mappingModel != null) {
            logger.info("물리카테고리 갯수: {}", mappingModel.keySet().size());
            categoryScheduler = new CategoryScheduler(mappingModel);
        }
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> item) {
        // 옵션들
        String makerCodeStr = (String) item.getOrDefault("makerCode", null);
        String brandCodeStr = (String) item.getOrDefault("brandCode", null);
        Integer makerCode = makerCodeStr == null ? null : Integer.parseInt(makerCodeStr);
        Integer brandCode = brandCodeStr == null ? null : Integer.parseInt(brandCodeStr);

        // attr 123,123,123
        String[] nAttributeValueSeqStrList = ((String) item.getOrDefault("nAttributeValueSeq", "")).split(",");
        List<Integer> nAttributeValueSeqList = new ArrayList<>();
        for (String attr : nAttributeValueSeqStrList) {
            nAttributeValueSeqList.add(Integer.parseInt(attr));
        }

        Set<Map<String, Object>> mapping = null;
        if (makerCode != null && brandCode != null) {
            // 찾아본다.
            for (int i = 4; i >= 1; i--) {
                int code = Integer.parseInt(item.getOrDefault("categoryCode" + i, "0").toString());
                if (code != 0) {
                    try {
                        mapping = categoryScheduler.findUICategoryList(code, nAttributeValueSeqList, makerCode, brandCode);
                        if (mapping.size() > 0) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.error("{}   {}   {}   {} ", code, nAttributeValueSeqList, makerCode, brandCode);
                        logger.error("", e);
                    }

                }
            }
        }

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
