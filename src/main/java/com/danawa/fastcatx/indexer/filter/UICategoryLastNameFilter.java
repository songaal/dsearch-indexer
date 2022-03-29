package com.danawa.fastcatx.indexer.filter;

import com.danawa.convertcategory.CategoryScheduler;
import com.danawa.convertcategory.entity.CategoryMappingModel;
import com.danawa.convertcategory.entity.UICategory;
import com.danawa.fastcatx.indexer.Filter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;
import org.springframework.web.client.RestTemplate;

import java.lang.reflect.Type;
import java.util.*;
import java.util.stream.Collectors;

public class UICategoryLastNameFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(UICategoryLastNameFilter.class);
    private CategoryScheduler categoryScheduler;
    private final RestTemplate restTemplate = new RestTemplate();
    private List<String> ignoreCategoryChar = null;

    @Override
    public void init(Map<String, Object> payload) {
        String categoryMappingUrl = (String) payload.getOrDefault("categoryMappingUrl", "");
        String ignoreCategoryCharStr = (String) payload.getOrDefault("ignoreCategoryChar", "");
        ignoreCategoryChar = Arrays.asList(ignoreCategoryCharStr.split(","));

        logger.info("UI카테고리 맵핑 조회 URL: {}", categoryMappingUrl);
        logger.info("UI카테고리 제외, 무시할 문자: {}", ignoreCategoryChar);
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
        String makerCodeStr = null;
        String brandCodeStr = null;
        Integer makerCode = null;
        Integer brandCode = null;
        List<Integer> nAttributeValueSeqList = new ArrayList<>();
        Integer code = null;

        try {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            // 옵션들
            makerCodeStr = (String) item.getOrDefault("makerCode", null);
            brandCodeStr = (String) item.getOrDefault("brandCode", null);
            makerCode = makerCodeStr == null ? null : Integer.parseInt(makerCodeStr);
            brandCode = brandCodeStr == null ? null : Integer.parseInt(brandCodeStr);

            // attr 123,123,123
            String[] nAttributeValueSeqStrList = ((String) item.getOrDefault("nAttributeValueSeq", "")).split(",");
            for (String attr : nAttributeValueSeqStrList) {
                if (!"".equals(attr)) {
                    nAttributeValueSeqList.add(Integer.parseInt(attr));
                }
            }

            Set<Map<String, Object>> mapping = null;
            // 찾아본다.
            for (int i = 4; i >= 1; i--) {
                code = Integer.parseInt(item.getOrDefault("categoryCode" + i, "0").toString());
                if (code != 0) {
                    mapping = categoryScheduler.findUICategoryList(code, nAttributeValueSeqList, makerCode, brandCode);
                    if (mapping.size() > 0) {
                        break;
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
                List<Integer> codeList = new ArrayList<>();
                List<String> lastNameList = new ArrayList<>();
                for (Map<String, Object> tmp : mapping) {
                    UICategory uiCategory = (UICategory) tmp.get("uiCategory");
                    String lastCategoryName = uiCategory.getName().get(uiCategory.getName().size() - 1);
                    boolean isInclude = false;
                    for (String c : ignoreCategoryChar) {
                        if (lastCategoryName != null && lastCategoryName.contains(c)) {
                            isInclude = true;
                            break;
                        }
                    }
                    if (!isInclude) {
                        codeList.add(uiCategory.getCode());
                        lastNameList.add(lastCategoryName);
                    }
                }
                item.put("uiCategoryCode", codeList);
                item.put("uiCategoryName", lastNameList);
            } else {
                item.put("uiCategoryCode", new ArrayList<>());
                item.put("uiCategoryName", new ArrayList<>());
            }
            stopWatch.stop();
            long et = stopWatch.getTotalTimeMillis();
            if (et > 10 * 1000) {
                logger.debug("UI 카테고리 소요시간: {}", et);
            }
        } catch (Exception e) {
            logger.error("phCateCode: {},   maker: {},   brand:{},  attr: {} ", code, makerCode, brandCode, nAttributeValueSeqList);
            logger.error("", e);
        }
        return item;
    }


}
