package com.danawa.fastcatx.indexer.ingester;

import com.danawa.fastcatx.indexer.*;
import com.github.fracpete.processoutput4j.output.ConsoleOutputProcessOutput;
import com.github.fracpete.rsync4j.RSync;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class ProcedureIngester extends FileIngester {

    private Type entryType;
    private Gson gson;
    private boolean isRun;

    public ProcedureIngester(String filePath, String encoding, int bufferSize) {
        this(filePath, encoding, bufferSize, 0);
    }

    public ProcedureIngester(String filePath, String encoding, int bufferSize, int limitSize) {

        super(filePath, encoding, bufferSize, limitSize);
        logger.info(filePath);
        gson = new Gson();
        entryType = new TypeToken<Map<String, Object>>() {}.getType();
        isRun = true;

    }


    @Override
    protected void initReader(BufferedReader reader) throws IOException {

    }

    @Override
    protected Map<String, Object> parse(BufferedReader reader) throws IOException {
        String line="";
        
        //종료 체크용 카운트
        int waitCount = 0;
        while (isRun) {
            try {
                line = reader.readLine();
                if(line != null) {
                    waitCount=0;
                    Map<String, Object> record = gson.fromJson(Utils.convertKonanToNdJson(line), entryType);

                    //정상이면 리턴.
                    return record;
                }else{
                    //대기 상태가 연속으로 X회 이상이면 반복 중지
                    if(waitCount > 20) {
                        stop();
                    }
                    logger.info(("wait"));
                    waitCount++;
                    Thread.sleep(1000);
                }
            }catch(Exception e) {

                logger.error("parsing error : line= " + line, e);
            }
        }
        throw new IOException("EOF");
    }

    public void stop() {
        isRun =false;
    }

}
