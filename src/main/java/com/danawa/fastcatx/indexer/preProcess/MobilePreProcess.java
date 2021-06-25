package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MobilePreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(MobilePreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    private final RestTemplate restTemplate = new RestTemplate();
    private final Gson gson = new Gson();

    public MobilePreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("MOBILE 전처리를 시작합니다.");
        String searchType = (String) payload.getOrDefault("searchType", "");
        String categorySearchUrl = (String) payload.getOrDefault("categorySearchUrl", "");
        String categorySearchBody = (String) payload.getOrDefault("categorySearchBody", "");
        String categoryXmlFilePath = (String) payload.getOrDefault("categoryXmlFilePath", "");
        String refreshApiUri = (String) payload.getOrDefault("refreshApiUri", "");

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        HttpEntity<String> httpEntity;
        HttpMethod method;
        if ("fastcat".equalsIgnoreCase(searchType)) {
            method = HttpMethod.GET;
            httpEntity = new HttpEntity<>(headers);
        } else {
            method = HttpMethod.POST;
            httpEntity = new HttpEntity<>(categorySearchBody, headers);
        }
        ResponseEntity<String> response = restTemplate.exchange(categorySearchUrl, method, httpEntity, String.class);
        Map<String, Object> body = gson.fromJson(response.getBody(), new TypeToken<HashMap<String, Object>>(){}.getType());
        List<Map<String, Object>> categories = new ArrayList<>();
        if (body != null && body.get("result") != null) {
            categories = (List<Map<String, Object>>) body.get("result");
        } else if (body != null && body.get("hits") != null) {
            Map<String, Object> hitsMap = (Map<String, Object>) body.get("hits");
            categories = (List<Map<String, Object>>) hitsMap.get("hits");
        } else {
            logger.warn("카테고리 조회 결과가 없습니다. body: {}", body);
        }

        deleteCategoryXml(categoryXmlFilePath);


        String categoryCode = "";
        String categoryName = "";
        for (Map<String, Object> category : categories) {
            if ("fastcat".equalsIgnoreCase(searchType)) {
                if (category.get("DEPTH").equals("1")) {
                    categoryCode = category.get("CATEGORYCODE").toString();
                    categoryName = category.get("CATEGORYNAME").toString();
                } else {
                    categoryCode = category.get("PCATEGORYCODE").toString() +
                            "_" + category.get("CATEGORYCODE").toString();
                    for (Map<String, Object> pcategory : categories) {
                        if (category.get("PCATEGORYCODE").toString().equals(pcategory.get("CATEGORYCODE"))){
                            categoryName = pcategory.get("CATEGORYNAME").toString();
                            break;
                        }
                    }
                    categoryName += ">" + category.get("CATEGORYNAME").toString();
                }
            } else {
                Map<String, Object> source = (Map<String, Object>) category.get("_source");
                if (source.get("depth").equals("1")) {
                    categoryCode = source.get("categoryCode").toString();
                    categoryName = source.get("categoryName").toString();
                } else {
                    categoryCode = source.get("pCategoryCode").toString() +
                            "_" + source.get("categoryCode").toString();
                    for (Map<String, Object> pcategory : categories) {
                        Map<String, Object> psource = (Map<String, Object>) pcategory.get("_source");
                        if (source.get("pCategoryCode").toString().equals(psource.get("categoryCode"))){
                            categoryName = psource.get("categoryName").toString();
                            break;
                        }
                    }
                    categoryName += ">" + source.get("categoryName").toString();
                }
            }
            updateCategoryXml(categoryXmlFilePath, categoryCode, categoryName);
        }
        logger.info("로그분석기용 카테고리 재구성 완료");

        refreshCategoryInfo(refreshApiUri);

        logger.info("MOBILE 전처리를 완료하였습니다.");
    }

    private void deleteCategoryXml(String xmlFilePath) {
        logger.info("기존 카테고리 삭제");
        File file = new File(xmlFilePath);
        try {
            //Create instance of DocumentBuilderFactory
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            //Get the DocumentBuilder
            DocumentBuilder docBuilder = factory.newDocumentBuilder();

            //Parsing XML Document
            Document doc = docBuilder.parse(file);
            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            XPathExpression expr = xpath.compile("//category-list");
            Node categoryList = (Node) expr.evaluate(doc, XPathConstants.NODE);

            categoryList.getParentNode().removeChild(categoryList);

            XPathExpression expr2 = xpath.compile("//ctr");
            Node ctr = (Node) expr2.evaluate(doc, XPathConstants.NODE);

            Element newCategoryList = doc.createElement("category-list");
            Node newCategoryNode = (Node) newCategoryList;

            doc.getFirstChild().insertBefore(newCategoryNode, ctr);

            Element rootCategory = doc.createElement("category");
            rootCategory.setAttribute("useRelateKeyword", "ture");
            rootCategory.setAttribute("usePopularKeyword", "true");
            rootCategory.setAttribute("useRealTimePopularKeyword", "ture");
            rootCategory.setAttribute("id", "_root");
            rootCategory.setAttribute("name", "ALL");
            Node rootCategoryNode = (Node) rootCategory;

            Element totalCategory = doc.createElement("category");
            totalCategory.setAttribute("useRelateKeyword", "false");
            totalCategory.setAttribute("usePopularKeyword", "true");
            totalCategory.setAttribute("useRealTimePopularKeyword", "false");
            totalCategory.setAttribute("id", "0");
            totalCategory.setAttribute("name", "전체");
            Node totalCategoryNode = (Node) totalCategory;

            newCategoryList.appendChild(rootCategoryNode);
            newCategoryList.appendChild(totalCategoryNode);

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer xformer = transformerFactory.newTransformer();
            xformer.transform(new DOMSource(doc), new StreamResult(new File(xmlFilePath)));

        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void updateCategoryXml(String xmlFilePath, String categoryCode, String categoryName) {
        logger.info(categoryCode + " " + categoryName + " 추가");
        File file = new File(xmlFilePath);
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = factory.newDocumentBuilder();
            factory.setIgnoringElementContentWhitespace(true);
            Document doc = docBuilder.parse(file);
            XPathFactory xpathFactory = XPathFactory.newInstance();
            XPath xpath = xpathFactory.newXPath();
            XPathExpression expr = xpath.compile("//category-list");
            Node categoryList = (Node) expr.evaluate(doc, XPathConstants.NODE);

            Element newCategory = doc.createElement("category");
            newCategory.setAttribute("useRelateKeyword", "false");
            newCategory.setAttribute("usePopularKeyword", "true");
            newCategory.setAttribute("useRealTimePopularKeyword", "false");
            newCategory.setAttribute("id", categoryCode);
            newCategory.setAttribute("name", categoryName);
            Node newCategoryNode = (Node) newCategory;

            categoryList.appendChild(newCategoryNode);

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer xformer = transformerFactory.newTransformer();
            xformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
            xformer.setOutputProperty(OutputKeys.INDENT, "yes");
            xformer.transform(new DOMSource(doc), new StreamResult(new File(xmlFilePath)));
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    public void refreshCategoryInfo(String refreshApiUri) throws Exception {
        logger.info("로그분석기 반영 API 호출");

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        HttpEntity<String> httpEntity = new HttpEntity<>(headers);
        ResponseEntity<Map> response = restTemplate.exchange(refreshApiUri, HttpMethod.GET, httpEntity, Map.class);
        Map<String, Object> body = response.getBody();

        if("true".equalsIgnoreCase(String.valueOf(body.get("success")))) {
            logger.info("로그분석기 반영 성공");
        } else {
            logger.error("로그분석기 반영 실패");
            throw new Exception("LogAnalytics Refresh Fail.");
        }
    }
}
