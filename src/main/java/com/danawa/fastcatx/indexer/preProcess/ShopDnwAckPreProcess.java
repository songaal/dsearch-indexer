package com.danawa.fastcatx.indexer.preProcess;

import com.danawa.fastcatx.indexer.entity.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShopDnwAckPreProcess implements PreProcess {
    private static final Logger logger = LoggerFactory.getLogger(ShopDnwAckPreProcess.class);
    private Job job;
    private Map<String, Object> payload;

    public ShopDnwAckPreProcess(Job job) {
        this.job = job;
        this.payload = job.getRequest();
    }

    @Override
    public void start() throws Exception {
        logger.info("SHOPDNWACK 전처리를 시작합니다.");
//        /data/product/export/text/SHOPDNWACK/ShopDnwKeyword.txt
        String acKeywordTxtFilePath = (String) payload.getOrDefault("acKeywordTxtFilePath", "");
//        /data/Application/analytics/statistics
        String statisticsPath = (String) payload.getOrDefault("statisticsPath", "");
//        /data/product/export/text/SHOPDNWACK/ShopDnwKeyword.txt
        String outputFilePath = (String) payload.getOrDefault("outputFilePath", "");
//        gather_SHOPDNW.sql
        String selectSql = (String) payload.getOrDefault("selectSql", "");
        String searchDBDriver = (String) payload.getOrDefault("searchDBDriver", "");
        String searchDBAddress = (String) payload.getOrDefault("searchDBAddress", "");
        String searchDBUsername = (String) payload.getOrDefault("searchDBUsername", "");
        String searchDBPassword = (String) payload.getOrDefault("searchDBPassword", "");
//        1.5
        Double standardCount = (Double) payload.getOrDefault("standardCount", 1.5);
//        /data/product/export/text/SHOPDNWACK/AutoCompleteKeyword.dump.txt
        String fastcatSavePath = (String) payload.getOrDefault("fastcatSavePath", "");
//        /data/product/export/text/SHOPDNWACK/AutoCompleteKeyword.json
        String elasticSavePath = (String) payload.getOrDefault("elasticSavePath", "");

        Map<String, String[]> accKeywordResultMap = getAccureNewKeyword_n(statisticsPath, outputFilePath, getAccureKeyword(acKeywordTxtFilePath));

        DatabaseConnector databaseConnector = new DatabaseConnector();
        databaseConnector.addConn(searchDBDriver, searchDBAddress, searchDBUsername, searchDBPassword);
        try (Connection connection = databaseConnector.getConn()) {
            //
            HashMap<String, Integer> productNameMap = getProductNameForAC(connection, selectSql); // 공통

            // 검색횟수 수집 기준
            logger.info("수집 검색 횟수 : " + standardCount);

            // 색인용 DUMP FILE 생성
            makeDumpFileList(fastcatSavePath, standardCount, accKeywordResultMap, productNameMap); // Fastcat
            // 파일 json 변환
            dumpToJson(fastcatSavePath, elasticSavePath);
            logger.info("자동완성 파일 dump->json 파일 변환 완료");
        }

        logger.info("SHOPDNWACK 전처리를 완료하였습니다.");
    }


    /**
     * 로그분석기로 쌓인 전날 raw.log 파일을 읽어 기존 누적된 키워드에 누적함
     *
     * @param map
     * @return map
     * @throws IOException
     */
    public Map<String, String[]> getAccureNewKeyword_n(String statisticsPath, String outputFilePath, Map<String, String[]> map)
            throws IOException {
        logger.info("누적키워드와 신규키워드 누적 프로세스 시작.");
        HashMap<String, String[]> keywordMap;

        keywordMap = new HashMap<>();
        Calendar cal = new GregorianCalendar();
        // 어제 날짜의 raw.log 파일
        cal.add(Calendar.DATE, -1);

        int year = cal.get(Calendar.YEAR);
        String month = String.format("%02d", cal.get(Calendar.MONTH) + 1);
        String day = String.format("%02d", cal.get(Calendar.DAY_OF_MONTH));
        String inputFilePath = String.format("%s/shopdnw/date/Y%d/M%s/D%s/data/raw.log", statisticsPath, year, month, day);


        Map<String, Double> newCountMap = new HashMap<>();
        BufferedReader inputFileBufferedReader = null;
        try {
            inputFileBufferedReader = new BufferedReader(new FileReader(inputFilePath));
            String rline;

            // 어제 로그에 대한 키워드별 MAP 생성
            while ((rline = inputFileBufferedReader.readLine()) != null) {
                // ex ) 13:40 860 노트북삼성 동화책 877995 1 search
                /*
                 * tempstr[0] 시간 tempstr[1] 카테고리 코드 tempstr[2] 검색 키워드 tempstr[3] 이전 검색 키워드
                 * tempstr[4] 결과 갯수 tempstr[5] 속도
                 */
                //

                String[] tempstr = rline.split("\t");
                if (tempstr.length > 4) { // count[1] = 검색결과 유무
                    String[] count = new String[3];

                    // 검색어 - map에 저장된 키워드는 공백이 제거된 키워드
                    String searchKey = tempstr[2].toLowerCase().trim();
                    String key = searchKey.replace(" ", "");

                    // 검색갯수가 숫자일때만 수행
                    if (checkIntegerData(tempstr[4])) {

                        // 키워드 검색 결과 갯수
                        Double searchResultCount = Double.parseDouble(tempstr[4]);
                        // 기존에 누적키워드 리스트에 있을 경우
                        if (map.get(key) != null && map.get(key)[0] != null) {
                            // System.out.println(key + " : " + (map.get(key)[0]));
                            // System.out.println(key + " : " + map.get(key).length + " / " +
                            // map.get(key)[1]);

                            // 검색 횟수
                            Double searchCount = Double.parseDouble(map.get(key)[0]);

                            // 누적 프로세스에 키워드당 일일 검색횟수가 + 되기때문에
                            // 신규 검색횟수는 따로 저장함
                            // 각 키워드마다 +1 씩하여 해당날 키워드당 검색횟수를 저장
                            if (newCountMap.get(key) != null) {
                                newCountMap.put(key, newCountMap.get(key) + 1);
                            } else {
                                newCountMap.put(key, 1.0);
                            }

                            // 누적 검색횟수 있는 경우
                            if (searchCount > 0.005) {
                                count[0] = setNumber(searchCount);
                                count[2] = map.get(key)[2];

                                // 기존에 검색 결과 유무가 Y면 그대로 Y 처리
                                // - 조건에 따라 검색결과가 0으로 기록될 수 있기 때문에 한번이라도 검색결과가 있는 키워드는 Y로 처리한다.
                                // - N이어도 그날 검색 결과가 있다면 Y로 처리
                                if (map.get(key)[1].equals("Y")) {
                                    count[1] = "Y";
                                } else {
                                    if (key.trim().length() > 0 && searchResultCount > 0) {
                                        count[1] = "Y";
                                    } else {
                                        count[1] = "N";
                                    }
                                }

                                /*
                                 * if(key.trim().length() > 0 && searchResultCount > 0.0){ count[1] = "Y"; }else
                                 * if(key.trim().length() > 0 && searchResultCount == 0){ //기존에 검색횟수가 있었을 경우는 Y로
                                 * 처리 // - 조건에 따라 검색결과가 0으로 기록될 수 있기 때문에 한번이라도 검색결과가 있는 키워드는 Y로 처리한다.
                                 * if(map.get(key)[1].equals("Y")) { //System.out.println(key + " : " +
                                 * map.get(key)[1] + " : " + tempstr[4]); count[1] = "Y"; }else{ count[1] = "N";
                                 * } //System.out.println(key + " : " + count[0] + " : " + tempstr[4]); }
                                 */
                            }
                            /*
                             * else{ count[0] = "0"; count[1] = "N"; }
                             */
                            map.put(key, count);
                        } else {

                            /*
                             * 검색횟수가 있다 newCountMap에 등록됨? NO -> newCountMap에 입력 -> COUNT[0] = 1 -> COUNT[1]
                             * = Y
                             *
                             * YES -> newCountMap에는 검색횟수 누적 -> COUNT[0] = 누적 검색 횟수 -> COUNT[1] = Y
                             */

                            if (key.trim().length() > 0 && searchResultCount > 0) {

                                if (newCountMap.get(key) != null) {
                                    newCountMap.put(key, newCountMap.get(key) + 1);
                                    count[0] = newCountMap.get(key).toString();
                                    count[1] = "Y";
                                    count[2] = searchKey;
                                } else {
                                    newCountMap.put(key, 1.0);
                                    count[0] = "1";
                                    count[1] = "Y";
                                    count[2] = searchKey;
                                }

                            }

                            /*
                             * if(newCountMap.get(key) != null) { count[0] =
                             * newCountMap.get(key).toString(); newCountMap.put(key,newCountMap.get(key)+1);
                             * }else{ count[0] = "1"; newCountMap.put(key,1.0); }
                             *
                             * //기존에 검색 결과 유무가 Y면 그대로 Y 처리 // - 조건에 따라 검색결과가 0으로 기록될 수 있기 때문에 한번이라도 검색결과가 있는
                             * 키워드는 Y로 처리한다. // - N이어도 그날 검색 결과가 있다면 Y로 처리 if(map.get(key)[1].equals("Y")) {
                             * count[1] = "Y"; }else{ if(key.trim().length() > 0 && searchResultCount > 0) {
                             * count[1] = "Y"; }else{ count[1] = "N"; } }
                             */

                            map.put(key, count);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            if (inputFileBufferedReader != null) {
                try {
                    inputFileBufferedReader.close();
                } catch (Exception ignore) {}
            }
        }

        logger.info("누적 프로세스 진행. :  " + map.size());
        Iterator<Map.Entry<String, String[]>> accMap = map.entrySet().iterator();

        while (accMap.hasNext()) {

            Map.Entry<String, String[]> entry = accMap.next();

            // 일일 누적 검색 갯수에 0.9를 곱함
            Double count = getSearchCount(entry.getValue()[0]);
            Double result = 0.0;

            // 일일 검색 개수를 더함
            if (newCountMap.get(entry.getKey().trim()) != null) {
                result = count + newCountMap.get(entry.getKey().trim());
                entry.getValue()[0] = setNumber(result);
            } else {
                entry.getValue()[0] = setNumber(count);
            }

            // 신규 검색 갯수
            if (entry.getKey().length() > 0 && Double.parseDouble(entry.getValue()[0]) > 0.005) {
                keywordMap.put(entry.getKey().trim(), entry.getValue());
            }
        }

        logger.info("누적키워드와 신규키워드 누적 프로세스 종료.");
        logger.info(String.format("유니크한 키워드 갯수 : %d", map.size()));

        // 파일 생성
        logger.info("키워드 누적 파일 생성 시작.");

        // 해당 키워드로 누적키워드 데이터에 추가반영함
        makeAccureKeywordFile(outputFilePath, keywordMap);
        logger.info("누적 키워드 파일 생성 종료.");
        return keywordMap;
    }


    private Map<String, String[]> getAccureKeyword(String filePath) throws IOException {
        Map<String, String[]> map = new LinkedHashMap<>();
        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(filePath));
            String rline = null;
            while ((rline = br.readLine()) != null) {
                // ex) skg2400 0.136 Y skg 2400
                /**
                 * tempstr[0] = 공백제거키워드 - (공백제거 유무에 따른 중복키워드 제거를 위함) tempstr[1] = 검색횟수 점수
                 * tempstr[2] = 검색결과 유무 여부 tempstr[3] = 검색 키워드
                 *
                 */
                String[] tempstr = rline.split("\t");

                if (tempstr[0].toLowerCase().trim().length() > 0) {
                    // 검색키워드
                    String key = tempstr[3].toLowerCase().trim();
                    // 공백제거키워드 = tempstr[0]
                    String replacekey = key.replace(" ", "");
                    // 검색 결과 여부
                    String isResultCount = tempstr[2];
                    // 검색횟수 점수
                    Double searchCount = Double.parseDouble(tempstr[1]);

                    // 조건 분기 이유
                    // 2019.08 , 공백에 의한 중복 키워드 제거 목적으로 처음만 실행됨(이후 null 아니
                    // ex). 누적키워드에서 삼성 노트북 , 삼성노트북 -> 높은 점수를 가진 키워드로 통합
                    if (map.get(replacekey) == null) {
                        // 갯수 카운트
                        String[] count = new String[3];

                        // 검색결과 유무 체크를 위한 기본값 'P' 셋팅
                        if (tempstr.length == 2) {
                            isResultCount = "P";
                        } else {
                            isResultCount = tempstr[2];
                        }

                        // 검색 횟수
                        count[0] = setNumber(searchCount);
                        // 검색결과 유무
                        count[1] = isResultCount;
                        // 키워드
                        count[2] = key;
                        // MAP [ 키워드 , [검색 횟수 , 검색 결과 유무, 검색키워드] ]
                        map.put(replacekey, count);
                    } else {
                        String[] count = map.get(replacekey);

                        Double sumCount = searchCount + Double.parseDouble(count[0]);

                        // 비교 키워드의 점수가 더 크면
                        // 비교 키워드로 배열값 교체
                        if (Double.parseDouble(count[0]) < Double.parseDouble(setNumber(searchCount))) {
                            count[0] = sumCount.toString();
                            count[2] = key;
                            map.put(replacekey, count);
                        } else {
                            count[0] = sumCount.toString();
                            map.put(replacekey, count);
                        }
                    }
                }
            }


        } catch (Exception e) {
            logger.error("", e);
            throw e;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ignore){}
            }

        }
        return map;
    }

    // double형의 소수점 .0 을 없앰
    // 소숫점은 3자리까지
    private String setNumber(double num) {
        DecimalFormat df = new DecimalFormat("#.###");
        return df.format(num);
    }

    private boolean checkIntegerData(String data) {

        Pattern pattern = Pattern.compile("(^[0-9]*$)");
        Matcher m = pattern.matcher(data);

        if (data != null) {
            if (data.length() > 0) {
                if (m.find()) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return false;
    }

    // 신규 누적카운트 프로세스
    /**
     *
     * Total Search Count : 누적검색수 New Search Count : 신규검색수 Total Search Count(D-0) =
     * Total Search Count(D-1) * 0.9 + New Search Count IF Total Search Count(D-0) <
     * 0.5 Then Del 소숫점 세자리까지
     */
    private Double getSearchCount(String totalSearchCount) {
        if (totalSearchCount != null) {
            Double searchCount = (Double.parseDouble(totalSearchCount));
            searchCount = Double.parseDouble(String.format("%.3f", searchCount));
            return searchCount;
        } else {
            return 0.0;
        }
    }

    public void makeAccureKeywordFile(String outputFilePath, Map<String, String[]> rowMap) throws IOException {
        try{
            logger.info("키워드 누적 데이타 생성 시작");

            BufferedWriter bw = new BufferedWriter(new FileWriter(outputFilePath));
            Iterator<Map.Entry<String, String[]>> i = rowMap.entrySet().iterator();

            while (i.hasNext()) {
                Map.Entry<String, String[]> entry = i.next();
                if(Double.parseDouble(entry.getValue()[0]) > 0.000) {
                    //공백제거된키워드	검색횟수	검색결과여부	검색키워드
                    bw.write(entry.getKey() + "\t" + entry.getValue()[0] + "\t" + entry.getValue()[1]+ "\t" + entry.getValue()[2]);
                    bw.newLine();
                }else{
                    System.out.println();
                }
            }

            bw.close();
            logger.info("키워드 누적 데이타 생성 종료");
        } catch(IOException e){
            logger.error("", e);
            throw e;
        }
    }

    // 가격비교 중인 기준상품명 데이타 HashMap
    public HashMap<String, Integer> getProductNameForAC(Connection connection, String selectSql) throws Exception {
        HashMap<String, Integer> map = new HashMap<>();
        logger.info("기준상품명 수집 시작. - ");

        try {
            PreparedStatement pstmt = connection.prepareStatement(selectSql);

            long queryStartTime = System.currentTimeMillis(); // 쿼리 시작 시간
            ResultSet rs = pstmt.executeQuery();
            long queryEndTime = System.currentTimeMillis(); // 쿼리 시작 시간
            double queryTime = (queryEndTime - queryStartTime);
            logger.info("쿼리시간 : " + queryTime);

            while (rs.next()) {
                // 1. 제조사명, 2. 브랜드명, 3.상품명, 4.인기점수
                String makerName = rs.getString(1) + " ";
                String brandName = rs.getString(2) + " ";
                String productName = rs.getString(3);
                int score = rs.getInt(4);

                // 키워드 생셩규칙 : 제조사 + 브랜드 + 상품명
                // 제조사 브랜드가 동일 할경우 제조사 하나만 키워드로 사용
                if (makerName.equals(brandName))
                    brandName = "";

                // 제조사 명이 '확인중' 일경우 제조사명 값을 없앰
                if (makerName.equals("확인중 "))
                    makerName = "";

                String keyword = makerName + brandName + productName;

                // 키워드와 인기점수 저장
                map.put(keyword, score);

            }
        } catch (SQLException e) {
            logger.error("", e);
            logger.info(URLDecoder.decode(e.getMessage(), "MS949"));
//            sms.sendSMS(volumeName + " 수집 실패");
            throw e;
        }
        logger.info("ProductKeyword 수집 완료.");

        return map;
    }

    /*
     * 태그방식의 DUMP 파일 생성 기준상품명 부터 데이타 생성 누적키워드는 기준상품명 이후 생성되게 하여 색인시 PK 중복제거를 통해
     * 제거되도록 한다 누적키워드 생성시 키워드별 카운트5개 이상인 항목만 생성한다. 기준상품은 MAP에 있는 모든데이타 생성 정렬에 필요한
     * RANGE는 누적키워드 카운트에 20만점을 더 부여하여 기준상품 보다상위에 나오게 한다
     */
    public void makeDumpFileList(String savePath, Double standardCount, Map<String, String[]> accKeywordMap, Map<String, Integer> productNameMap) throws IOException {
        File exportFile = new File(savePath);

        try {
            exportFile.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(exportFile));

            // 기준상품 write
            logger.info("기준상품 DUMP FILE 생성 시작");
            Iterator<Map.Entry<String, Integer>> product = productNameMap.entrySet().iterator();
            while (product.hasNext()) {
                Map.Entry<String, Integer> entry = product.next();

                // 2019-09-20 - 식품의약품안저처 : 식품안전관리 강화 협조 요청 공문의 건으로 인한 자동완성에서의 특정 키워드포함 제외처리
                if (!entry.getKey().contains("조개젓")) {
                    bw.write("<doc>");
                    bw.newLine();
                    bw.write("<KEYWORD>");
                    bw.newLine();
                    bw.write(entry.getKey());
                    bw.newLine(); // 키워드
                    bw.write("</KEYWORD>");
                    bw.newLine();
                    bw.write("<HIT>");
                    bw.newLine();
                    bw.write(String.format("%d", entry.getValue()));
                    bw.newLine(); // 상품 인기점수
                    bw.write("</HIT>");
                    bw.newLine();
                    bw.write("<RANGE>");
                    bw.newLine();
                    bw.write(String.format("%d", entry.getValue()));
                    bw.newLine(); // 정렬점수 : 상품인기 점수 (20만 이하)
                    bw.write("</RANGE>");
                    bw.newLine();
                    bw.write("</doc>");
                    bw.newLine();
                }
            }
            logger.info(String.format("기준상품 DUMP FILE 생성 종료 [총 갯수 : %d]", productNameMap.size()));

            // 누적 키워드 write
            logger.info("누적 키워드 DUMP DATA 생성 시작");
            Iterator<Map.Entry<String, String[]>> acKeyword = accKeywordMap.entrySet().iterator();

            int cnt = 0;
            while (acKeyword.hasNext()) {
                Map.Entry<String, String[]> entry = acKeyword.next();
                // 기준 갯수 이상 && 검색결과 N이 아닌것만 색인처리
                // 2019-09-20 - 식품의약품안저처 : 식품안전관리 강화 협조 요청 공문의 건으로 인한 자동완성에서의 특정 키워드포함 제외처리
                if (!entry.getValue()[2].contains("조개젓")) {
                    if (Double.parseDouble(entry.getValue()[0]) > standardCount && !entry.getValue()[1].equals("N") && !entry.getValue()[1].equals("T")) {
                        bw.write("<doc>");
                        bw.newLine();
                        bw.write("<KEYWORD>");
                        bw.newLine();
                        // 검색된 키워드를 색인해야하므로 배열 데이터 사용
                        bw.write(entry.getValue()[2]);
                        bw.newLine(); // 키워드
                        bw.write("</KEYWORD>");
                        bw.newLine();
                        bw.write("<HIT>");
                        bw.newLine();
                        // bw.write(String.format("%.2f", Double.parseDouble(entry.getValue()[0])));
                        // bw.newLine(); // 검색 횟수
                        // System.out.println(entry.getValue()[0]);
                        bw.write(entry.getValue()[0]);
                        bw.newLine(); // 검색 횟수
                        bw.write("</HIT>");
                        bw.newLine();
                        bw.write("<RANGE>");
                        bw.newLine();
                        // bw.write(String.format("%.2f", Double.parseDouble(entry.getValue()[0]
                        // +200000))); bw.newLine(); // 정렬점수 : 검색횟수 + 200000
                        bw.write(setNumber(Double.parseDouble(entry.getValue()[0]) + 200000));
                        bw.newLine(); // 정렬점수 : 검색횟수 + 200000
                        bw.write("</RANGE>");
                        bw.newLine();
                        bw.write("</doc>");
                        bw.newLine();
                        cnt++;
                    }
                }
            }
            logger.info(String.format("누적 키워드 DUMP FILE 생성 종료 [" + standardCount + "이상 갯수 : %d]", cnt));
            bw.close();

        } catch (IOException e) {
            logger.error("", e);
//            sms.sendSMS(e.getMessage());
            throw e;
        }
    }

    // dump 파일을 json 파일로 변환
    private void dumpToJson(String inpuFilePath, String outputFilePath) throws IOException {

        logger.info(inpuFilePath + " : " + outputFilePath);

        File file = new File(outputFilePath);
        FileWriter fw = new FileWriter(file, true);

        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(inpuFilePath));
        } catch (FileNotFoundException e) {
            throw e;
        }

        boolean flag = false;
        boolean keywordFlag = false;
        boolean hitFlag = false;
        boolean rangeFlag = false;

        String keyword = null;
        String hit = null;
        String range = null;

        long count = 0;
        while (scanner.hasNext()) {

            try {
                String line = scanner.nextLine();
                if("<doc>".equals(line)){
                    flag = true;
                }else if("</doc>".equals(line)){
                    keyword = keyword.replace("\"", "").replace("\\", "");
                    String search = makeSearchKeyword(keyword).replace("\"", "").replace("\\", "");
//                    fw.write("{\"index\": {}}\n");
                    fw.write("{\"keyword\": \""+ keyword+ "\", \"hit\": "+ hit + ", \"range\": " + range + ", \"search\": \"" + search + "\"}\n");
                    count++;
                    if(count % 10000 == 0) logger.info(count + "개 완료 되었습니다");
                    flag = false;
                }else if("<KEYWORD>".equals(line)){
                    keywordFlag = true;
                }else if("</KEYWORD>".equals(line)){
                    keywordFlag = false;
                }else if("<HIT>".equals(line)){
                    hitFlag = true;
                }else if("</HIT>".equals(line)){
                    hitFlag = false;
                }else if("<RANGE>".equals(line)){
                    rangeFlag = true;
                }else if("</RANGE>".equals(line)){
                    rangeFlag = false;
                } else {
                    if(flag){
                        if(keywordFlag){
                            keyword = line;
                        }else if(hitFlag){
                            hit = line;
                        }else if(rangeFlag){
                            range = line;
                        }
                    }else{
                        // do nothing...
                    }
                }

            } catch (Exception e) {
                logger.error("ERR : " + e.getMessage());
                throw e;
            }
        }

        if(flag){
            keyword = keyword.replace("\"", "").replace("\\", "");
            String search = makeSearchKeyword(keyword).replace("\"", "").replace("\\", "");
            fw.write("{\"keyword\": \""+ keyword+ "\", \"hit\": "+ hit + ", \"range\": " + range + ", \"search\": \"" + search + "\"}\n");
        }


    }

    public String makeSearchKeyword(String keyword){
        String[] keywordArray = keyword.split("[ \t\n\r]");
        StringBuilder sb = new StringBuilder();
        for(String splitKeyword : keywordArray) {
            sb.append(makeHangulPrefix(splitKeyword, ' '));
            if(sb.length() > 0 && sb.charAt(sb.length() - 1) != ' '){
                sb.append(' ');
            }
            sb.append(makeHangulChosung(splitKeyword, ' '));
            if(sb.length() > 0 && sb.charAt(sb.length() - 1) != ' '){
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    /*
     * 검색엔진 fastcat -> es 변경으로 인한
     * ACKEYWORD, SHOPDNWACK 수집파일 형식 변경
     * dump -> json 변경 로직 추가
     */
    public static final String CHOSUNG_LIST = "ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ"; //19
    public static final String JUNGSUNG_LIST = "ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ"; //21
    public static final String JONGSUNG_LIST = " ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ"; //28
    private static final char unicodeHangulBase = '\uAC00';
    private static final char unicodeHangulLast = '\uD7A3';
    public String makeHangulPrefix(String keyword, char delimiter) {
        StringBuffer candidate = new StringBuffer();
        StringBuffer prefix = new StringBuffer();
        for (int i = 0; i < keyword.length(); i++) {
            char ch = keyword.charAt(i);
            if (ch < unicodeHangulBase || ch > unicodeHangulLast) {
                prefix.append(ch);
                candidate.append(prefix);
            } else {
                // Character is composed of {Chosung+Jungsung} OR
                // {Chosung+Jungsung+Jongsung}
                int unicode = ch - unicodeHangulBase;
                int choSung = unicode / (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                // 1. add prefix+chosung
                candidate.append(prefix);
                candidate.append(CHOSUNG_LIST.charAt(choSung));
                candidate.append(delimiter);
                // 2. add prefix+chosung+jungsung
                unicode = unicode % (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                int jongSung = unicode % JONGSUNG_LIST.length();
                char choJung = (char) (ch - jongSung);
                candidate.append(prefix);
                candidate.append(choJung);
                // change prefix
                prefix.append(ch);
                if (jongSung > 0) {
                    candidate.append(delimiter);
                    // 3. add whole character
                    candidate.append(prefix);
                }
            }
            if (i < keyword.length() - 1)
                candidate.append(delimiter);
        }
        return candidate.toString();
    }

    public String makeHangulChosung(String keyword, char delimiter) {
        StringBuffer candidate = new StringBuffer();
        StringBuffer prefix = new StringBuffer();
        for (int i = 0; i < keyword.length(); i++) {
            char ch = keyword.charAt(i);
            if (ch >= unicodeHangulBase && ch <= unicodeHangulLast) {
                int unicode = ch - unicodeHangulBase;
                int choSung = unicode / (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                candidate.append(prefix);
                candidate.append(CHOSUNG_LIST.charAt(choSung));

                if (i < keyword.length() - 1) {
                    candidate.append(delimiter);
                }

                prefix.append(CHOSUNG_LIST.charAt(choSung));
            }
        }
        return candidate.toString();
    }
}
