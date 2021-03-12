package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.FileIngester;
import com.danawa.fastcatx.indexer.Utils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class ProcedureIngester extends FileIngester {

    private Type entryType;
    private Gson gson;
    private String dumpFormat;
    private boolean isRun;
    private StringBuilder sb;

    public ProcedureIngester(String filePath, String dumpFormat, String encoding, int bufferSize, int limitSize) {

        super(filePath, encoding, bufferSize, limitSize);
        logger.info("filePath : {}", filePath);
        gson = new Gson();
        entryType = new TypeToken<Map<String, Object>>() {}.getType();
        isRun = true;
        sb = new StringBuilder();

        this.dumpFormat = dumpFormat;
    }


    @Override
    protected void initReader(BufferedReader reader) throws IOException {

    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line="";
        //종료 체크용 카운트
        int waitCount = 0;
        String startStr="";
        String endStr="";
        while (isRun) {
            try {

                //byte X -> char
                line = reader.readLine();
                if(line != null) {
                    waitCount=0;
                    //TODO String dumpFormat,
                    Map<String, Object> record = new HashMap<>();

                    //dumpFormat에 따라 체크하는 텍스트를 지정해준다.
                    if(dumpFormat.equals("konan")){
                        startStr = "[%productCode%]";
                        //endStr= "[%modifyDate%]";
                        endStr= "[%feeMallType%]";
                    }else if(dumpFormat.equals("ndjson")){
                        startStr = "{\"productCode\":";
                        //endStr = "\"modifyDate\":";
                        endStr = "\"feeMallType\":";
                    }

                    //추후 개행문자로 구분하도록 수정 예정
                    //시작,끝 필드텍스트가 모두 포함되어 있으면 dumpFormat에 따라 ndjson 변환 혹은 그대로 반환
                    if(line.contains(startStr) && line.contains(endStr)) {

                        if(dumpFormat.equals("konan")){
                            record = gson.fromJson(Utils.convertKonanToNdJson(line), entryType);
                        }else if(dumpFormat.equals("ndjson")){
                            record = gson.fromJson(line, entryType);
                        }

                        // 비정상 상품 ROW 이후 정상 상품ROW가 읽혔을 때, sb 초기화
                        if(sb.length() > 0) sb.setLength(0);

                    }else{
                        //정상적인 상품 ROW가 아니면 StringBuilder에 append
                        logger.debug("append line : {}", line);
                        sb.append(line);
                    }

                    //append된 StringBuilder가 시작, 끝 텍스트를 포함하고 있으면 반환 후 StringBuilder 초기화
                    if(sb.toString().contains(startStr) && sb.toString().contains(endStr)) {
                        logger.debug("sb : {}", sb.toString());

                        if(dumpFormat.equals("konan")){
                            record = gson.fromJson(Utils.convertKonanToNdJson(sb.toString()), entryType);
                        }else if(dumpFormat.equals("ndjson")){
                            record = gson.fromJson(sb.toString(), entryType);
                        }
                        sb.setLength(0);
                    }else if(sb.length() > startStr.length() && sb.toString().indexOf(startStr) != 0) {
                        //문서 ROW의 시작은 항상 상품코드이므로 상품코드가 먼저 시작되지 않았다면 초기화
                        //sb.length 체크 이유는 상품코드 텍스트가 짤렸을때 초기화 되는것을 방지( ex. [%PRODU  )
                        logger.debug("reset sb : {} ", sb.toString());
                        sb.setLength(0);
                    }

                    return record;
                }else{
                    //대기 상태가 연속으로 X회 이상이면 반복 중지
                    // FIXME rsync 전송이 갑자기 느려져서 20초간 한문서도 전송받지 못하거나
                    // 그외 어떤 이유(시스템 부하)로 잠시 멈출때는 전송완료와 구별하지 못함.
                    if(waitCount > 20) {
                        stop();
                    }
                    logger.info(("wait"));
                    waitCount++;
                    Thread.sleep(1000);
                }
            }catch(Exception e) {
                logger.error("parsing error : line = {}, \nconvert = {}, \nsb = {}", line, Utils.convertKonanToNdJson(line), sb.toString());
                logger.error("{}", e);
                // 에러 발생 시 sb 에 쌓인 데이터 제거
                if(sb.length() > 0) sb.setLength(0);
//                logger.error("parsing error : line= " + line, e);
            }
        }
        throw new IOException("EOF");
    }


    public void stop() {
        isRun =false;
    }

}
